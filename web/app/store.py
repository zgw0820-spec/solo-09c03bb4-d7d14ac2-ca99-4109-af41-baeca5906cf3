"""SQLite persistence and state machine for the operations web.

The `commands` table is the single source of truth.  Every transition is a
conditional UPDATE inside a ``BEGIN IMMEDIATE`` transaction, so concurrent
submissions and the dispatcher can never leave a terminal state or deliver the
same command twice.

States
------
pending     accepted, waiting for its turn on the device queue (never sent).
inflight    handed to the network at least once; either on the wire, or
            waiting for a receipt to be verified by command id.  This is NOT
            a failure, and it is NOT terminal even after its deadline.
executed    gateway verified: it ran and succeeded.          (terminal)
failed      gateway verified: it ran and failed.             (terminal)
unexecuted  verified it never ran (deadline passed).         (terminal)
rejected    gateway refused the id (id conflict).            (terminal)

An overdue inflight command keeps blocking its device queue until the gateway
answers the verification query; it is never assumed to have failed.

`wire_state` records how far delivery got:
    NULL      nothing written
    claimed   moved to inflight, request not yet handed to the network
    sent      a request was (or may have been) written to the socket
Only `claimed` commands may be returned to the queue when the gateway says it
has no record.  A `sent` command with no gateway record stays inflight: bytes
may still be in transit, so "not received yet" is not "not executed".

Gateway concurrency is capped by *slots* (deliveries + verification probes
together).  `claim_batch` hands out slots inside one short transaction and
stamps each claimed row with `next_check_at`: for a delivery it is the slot
lease (a crashed dispatcher cannot wedge the device queue -- the row becomes
probe-able when the lease lapses), for a verification reservation it is the
earliest time the next probe may run.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS commands (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    command_id      TEXT NOT NULL UNIQUE,
    device_id       TEXT NOT NULL,
    deadline        TEXT NOT NULL,
    state           TEXT NOT NULL,
    receipt_status  TEXT,
    detail          TEXT,
    attempts        INTEGER NOT NULL DEFAULT 0,
    drop_receipt    INTEGER NOT NULL DEFAULT 0,
    receipt_lost    INTEGER NOT NULL DEFAULT 0,
    wire_state      TEXT,
    last_attempt_at TEXT,
    next_check_at   TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS by_device_state ON commands(device_id, state, id);
CREATE INDEX IF NOT EXISTS by_state ON commands(state);
CREATE INDEX IF NOT EXISTS by_state_check ON commands(state, next_check_at, id);
"""

TERMINAL_STATES = ("executed", "failed", "unexecuted", "rejected")
ACTIVE_STATES = ("pending", "inflight")

DISPLAY = {
    "pending": "等待下发",
    "inflight": "已发出，等待回执",
    "executed": "执行成功",
    "failed": "执行失败",
    "unexecuted": "未执行（已过期）",
    "rejected": "网关拒绝（指令号冲突）",
}


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def normalize_deadline(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def is_overdue(deadline: str, now: Optional[str] = None) -> bool:
    return (now or utcnow()) >= deadline


def lease_expiry(lease_seconds: float) -> str:
    """ISO timestamp ``lease_seconds`` in the future (delivery-slot deadline)."""
    return datetime.fromtimestamp(
        datetime.now(timezone.utc).timestamp() + max(0.0, lease_seconds),
        tz=timezone.utc,
    ).isoformat(timespec="microseconds")


class CommandIdConflict(Exception):
    """The command id already belongs to a different device."""


class Store:
    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock, self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            cols = {
                r["name"]
                for r in conn.execute("PRAGMA table_info(commands)").fetchall()
            }
            if "next_check_at" not in cols:  # upgrade databases from before slots
                conn.execute("ALTER TABLE commands ADD COLUMN next_check_at TEXT")

    # ------------------------------------------------------------------ reads
    def get(self, command_id: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()

    def list(self, limit: int = 300) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM commands ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()

    def inflight_due(self, *, now: str, limit: int) -> list[sqlite3.Row]:
        """Oldest inflight commands due for a verification probe.

        Bounded by ``limit``: with thousands of inflight rows the dispatcher
        never reads the whole backlog just to pick a few probes.  A row whose
        ``next_check_at`` lies in the future is skipped, so per-command probe
        pacing survives restarts and does not depend on in-memory state.
        """
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM commands WHERE state='inflight' "
                "AND (next_check_at IS NULL OR next_check_at <= ?) "
                "ORDER BY id LIMIT ?",
                (now, limit),
            ).fetchall()

    def counts(self) -> dict[str, int]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT state, COUNT(*) AS n FROM commands GROUP BY state"
            ).fetchall()
        return {r["state"]: r["n"] for r in rows}

    # ---------------------------------------------------------------- writes
    def submit(
        self,
        *,
        device_id: str,
        command_id: str,
        deadline: datetime,
        drop_receipt: bool = False,
    ) -> tuple[sqlite3.Row, bool]:
        """Idempotent submit keyed by command_id.

        Returns ``(row, created)``.  A duplicate returns the *original* row;
        nothing is inserted and no new execution can ever be triggered by it.
        A command already past its deadline is recorded as unexecuted at once.
        """
        deadline_s = normalize_deadline(deadline)
        now = utcnow()
        if deadline_s <= now:
            state, detail = "unexecuted", "截止时间已过，指令未下发"
        else:
            state, detail = "pending", "等待按设备顺序下发"

        with self._tx() as conn:
            existing = conn.execute(
                "SELECT * FROM commands WHERE command_id=?", (command_id,)
            ).fetchone()
            if existing is not None:
                if existing["device_id"] != device_id:
                    raise CommandIdConflict(
                        f"指令号 '{command_id}' 已属于设备 '{existing['device_id']}'"
                    )
                return existing, False

            conn.execute(
                "INSERT INTO commands (command_id, device_id, deadline, state, "
                "detail, drop_receipt, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    command_id,
                    device_id,
                    deadline_s,
                    state,
                    detail,
                    1 if drop_receipt else 0,
                    now,
                    now,
                ),
            )
            return (
                conn.execute(
                    "SELECT * FROM commands WHERE command_id=?", (command_id,)
                ).fetchone(),
                True,
            )

    def expire_pending(self) -> int:
        """Retire never-sent commands whose deadline has passed.

        Inflight rows are intentionally untouched: an unknown outcome must be
        verified with the gateway, not guessed.  A cheap read gates the write
        transaction so an idle tick does not take the write lock at all.
        """
        now = utcnow()
        with self.connect() as conn:
            if (
                conn.execute(
                    "SELECT 1 FROM commands WHERE state='pending' AND deadline <= ? "
                    "LIMIT 1",
                    (now,),
                ).fetchone()
                is None
            ):
                return 0
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET state='unexecuted', detail=?, updated_at=? "
                "WHERE state='pending' AND deadline <= ?",
                ("截止时间已过，指令未下发", now, now),
            )
            return cur.rowcount

    def claim_batch(
        self,
        *,
        retry_seconds: float = 1.0,
        max_slots: int = 4,
        in_flight: int = 0,
        scan_limit: int = 200,
        verify_limit: int = 2,
        verify_interval: float = 1.0,
        lease_seconds: float = 30.0,
    ) -> tuple[list[sqlite3.Row], list[sqlite3.Row]]:
        """Atomically hand out at most ``max_slots - in_flight`` gateway slots.

        A slot is either a delivery (pending -> inflight) or a verification
        probe reservation (inflight row stamped with a fresh ``next_check_at``
        lease).  Returns ``(deliveries, verifications)``.

        * Expired pending commands become unexecuted (never sent).
        * A device with an inflight command is blocked regardless of that
          command's deadline (outcome unknown => cannot pass it).
        * Only the oldest eligible pending command per device is claimed, so
          same-device commands run strictly in submission(order) = id order.
        * Device heads are visited in round-robin order keyed by the head's
          row id, so a device with a huge backlog cannot starve devices whose
          commands were submitted later.
        * Verification probes are interleaved with deliveries (one verify,
          one deliver, ...) so receipt verification is never starved either.
        * Both candidate pools are read with small LIMITs inside one short
          ``BEGIN IMMEDIATE`` transaction: the whole backlog is never scanned
          and the SQLite write lock is held only for a few index lookups.
        * A claimed delivery carries a lease (``next_check_at``); if the
          dispatcher dies before writing to the network, the row becomes
          probe-able once the lease lapses instead of blocking its device
          queue forever.
        """
        budget = max(0, max_slots - max(0, in_flight))
        if budget == 0:
            return [], []

        now = utcnow()
        cutoff = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - max(0.0, retry_seconds),
            tz=timezone.utc,
        ).isoformat(timespec="microseconds")
        lease_to = lease_expiry(lease_seconds)
        verify_to = lease_expiry(verify_interval)

        with self._tx() as conn:
            conn.execute(
                "UPDATE commands SET state='unexecuted', detail=?, updated_at=? "
                "WHERE state='pending' AND deadline <= ?",
                ("截止时间已过，指令未下发", now, now),
            )
            # Bounded candidate pools only -- never the whole backlog.
            heads = conn.execute(
                "SELECT * FROM ("
                "   SELECT *, MIN(id) OVER (PARTITION BY device_id) AS head_id"
                "   FROM commands WHERE state='pending'"
                ") WHERE id = head_id ORDER BY id LIMIT ?",
                (scan_limit,),
            ).fetchall()
            due = conn.execute(
                "SELECT * FROM commands WHERE state='inflight' "
                "AND (next_check_at IS NULL OR next_check_at <= ?) "
                "ORDER BY id LIMIT ?",
                (now, max(verify_limit, budget)),
            ).fetchall()

            deliveries: list[sqlite3.Row] = []
            verifications: list[sqlite3.Row] = []
            blocked: set[str] = set()  # devices claimed or backed off this pass
            inflight_devices: dict[str, bool] = {}  # exact per-device cache

            def device_blocked(device: str) -> bool:
                # Exact per-device check (indexed point query): a device with
                # an inflight command is blocked no matter how large the
                # global inflight backlog grows.
                if device not in inflight_devices:
                    inflight_devices[device] = (
                        conn.execute(
                            "SELECT 1 FROM commands WHERE device_id=? "
                            "AND state='inflight' LIMIT 1",
                            (device,),
                        ).fetchone()
                        is not None
                    )
                return inflight_devices[device]

            hi = 0  # round-robin cursor into device heads
            vi = 0  # cursor into due verifications
            # Alternate: one verification, then one device head, repeat.
            # This keeps receipt verification moving even when many devices
            # are queued, and lets many devices share the slots even when one
            # device has thousands of queued commands.
            while len(deliveries) + len(verifications) < budget:
                progressed = False
                if vi < len(due) and len(verifications) < verify_limit:
                    row = due[vi]
                    vi += 1
                    cur = conn.execute(
                        "UPDATE commands SET next_check_at=?, updated_at=? "
                        "WHERE id=? AND state='inflight'",
                        (verify_to, now, row["id"]),
                    )
                    if cur.rowcount == 1:
                        verifications.append(
                            conn.execute(
                                "SELECT * FROM commands WHERE id=?", (row["id"],)
                            ).fetchone()
                        )
                        progressed = True
                while hi < len(heads) and len(deliveries) + len(verifications) < budget:
                    row = heads[hi]
                    hi += 1
                    device = row["device_id"]
                    if device in blocked or device_blocked(device):
                        continue
                    if row["last_attempt_at"] and row["last_attempt_at"] > cutoff:
                        # Still in backoff (e.g. gateway unreachable).
                        blocked.add(device)
                        continue
                    cur = conn.execute(
                        "UPDATE commands SET state='inflight', wire_state='claimed', "
                        "attempts=attempts+1, last_attempt_at=?, next_check_at=?, "
                        "detail=?, updated_at=? WHERE id=? AND state='pending'",
                        (now, lease_to, "正在下发，等待网关回执", now, row["id"]),
                    )
                    if cur.rowcount == 1:
                        blocked.add(device)
                        deliveries.append(
                            conn.execute(
                                "SELECT * FROM commands WHERE id=?", (row["id"],)
                            ).fetchone()
                        )
                        progressed = True
                        break  # next slot goes to a verification / other device
                if not progressed:
                    break
            return deliveries, verifications

    def mark_sent(self, command_id: str) -> bool:
        """Write-ahead marker set immediately before the HTTP request.

        From here on the outcome is ambiguous and may only be resolved by
        querying the gateway for this command id.
        """
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET wire_state='sent', updated_at=? "
                "WHERE command_id=? AND state='inflight'",
                (utcnow(), command_id),
            )
            return cur.rowcount == 1

    def release_to_pending(self, command_id: str, detail: str) -> bool:
        """Only valid when no bytes were written (connect failure or a
        `claimed` row the gateway never saw).  The command returns to the
        queue and can still expire as unexecuted."""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET state='pending', wire_state=NULL, detail=?, "
                "updated_at=? WHERE command_id=? AND state='inflight' "
                "AND wire_state IN ('claimed', 'sent')",
                (detail, utcnow(), command_id),
            )
            return cur.rowcount == 1

    def reset_claimed_to_pending(self) -> int:
        """On startup, claims that never reached the network go back to the
        queue.  Rows marked `sent` are left alone: their outcome must be
        verified with the gateway."""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET state='pending', wire_state=NULL, "
                "detail=?, updated_at=? "
                "WHERE state='inflight' AND wire_state='claimed'",
                ("重启前未写出网络，重新排队", utcnow()),
            )
            return cur.rowcount

    def make_inflight_due(self) -> int:
        """On startup, make every inflight command immediately probe-able.

        Probing by command id is always safe, and after a restart nothing
        is known about outstanding leases, so the first verification pass
        should not wait for stale leases to lapse.  Pacing resumes with the
        lease each probe stamps on the row.
        """
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET next_check_at=NULL, updated_at=? "
                "WHERE state='inflight'",
                (utcnow(),),
            )
            return cur.rowcount

    def defer_check(self, command_id: str, delay_seconds: float) -> None:
        """Push this command's next verification probe into the future.

        Used to pace probes (after a lost receipt) and to re-arm the slot
        lease of a delivery whose outcome is still ambiguous.
        """
        with self._tx() as conn:
            conn.execute(
                "UPDATE commands SET next_check_at=?, updated_at=? "
                "WHERE command_id=? AND state='inflight'",
                (lease_expiry(delay_seconds), utcnow(), command_id),
            )

    def mark_unverified(self, command_id: str, detail: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE commands SET detail=?, updated_at=? "
                "WHERE command_id=? AND state='inflight'",
                (detail, utcnow(), command_id),
            )

    def mark_receipt_lost(self, command_id: str, detail: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE commands SET receipt_lost=1, detail=?, updated_at=? "
                "WHERE command_id=? AND state='inflight'",
                (detail, utcnow(), command_id),
            )

    def finish(
        self,
        command_id: str,
        *,
        state: str,
        receipt_status: Optional[str] = None,
        detail: Optional[str] = None,
    ) -> bool:
        """Conditional transition out of inflight; only the first wins."""
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET state=?, receipt_status=?, detail=?, "
                "updated_at=? WHERE command_id=? AND state='inflight'",
                (state, receipt_status, detail, utcnow(), command_id),
            )
            return cur.rowcount == 1

    def arm_drop_receipt(self, command_id: str) -> bool:
        """Arm receipt loss for the next delivery of this command.

        Only meaningful before delivery; for an already-delivered command the
        gateway's own drop-receipt endpoint is used instead.
        """
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE commands SET drop_receipt=1, updated_at=? "
                "WHERE command_id=? AND state IN ('pending','inflight')",
                (utcnow(), command_id),
            )
            return cur.rowcount == 1
