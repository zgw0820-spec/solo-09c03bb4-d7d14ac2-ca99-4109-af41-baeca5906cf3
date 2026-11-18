from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from web.app.store import CommandIdConflict, Store, lease_expiry, utcnow


@pytest.fixture()
def store(tmp_path):
    s = Store(tmp_path / "ops.db")
    s.init()
    return s


def deadline_in(seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def claim(store, **kw):
    """claim_batch returning only the delivery list (legacy test helper)."""
    kw.setdefault("retry_seconds", 0)
    kw.setdefault("max_slots", 64)
    kw.setdefault("verify_limit", 0)
    return store.claim_batch(**kw)[0]


def test_duplicate_submit_returns_original(store):
    row1, created1 = store.submit(
        device_id="d1", command_id="c1", deadline=deadline_in(60)
    )
    assert created1 is True
    claim(store)
    row2, created2 = store.submit(
        device_id="d1", command_id="c1", deadline=deadline_in(60)
    )
    assert created2 is False
    assert row2["id"] == row1["id"]
    assert row2["state"] == "inflight"
    assert row2["attempts"] == 1


def test_same_command_id_other_device_conflicts(store):
    store.submit(device_id="d1", command_id="c1", deadline=deadline_in(60))
    with pytest.raises(CommandIdConflict):
        store.submit(device_id="d2", command_id="c1", deadline=deadline_in(60))


def test_same_device_is_fifo_and_other_devices_parallel(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    store.submit(device_id="d1", command_id="a2", deadline=deadline_in(60))
    store.submit(device_id="d2", command_id="b1", deadline=deadline_in(60))

    claimed = claim(store)
    assert [r["command_id"] for r in claimed] == ["a1", "b1"]

    # a1 is still inflight -> a2 must not be claimed for d1
    assert claim(store) == []
    store.finish("a1", state="executed", receipt_status="succeeded")
    claimed = claim(store)
    assert [r["command_id"] for r in claimed] == ["a2"]


def test_inflight_blocks_queue_even_after_deadline(store):
    store.submit(
        device_id="d1", command_id="a1", deadline=deadline_in(0.2)
    )
    store.submit(device_id="d1", command_id="a2", deadline=deadline_in(60))
    claim(store)
    time.sleep(0.25)

    # a1's deadline has passed but it was sent: must stay inflight, still block.
    assert store.expire_pending() == 0
    assert store.get("a1")["state"] == "inflight"
    assert claim(store) == []
    assert store.get("a2")["state"] == "pending"


def test_pending_past_deadline_is_unexecuted(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(0.1))
    time.sleep(0.15)
    store.expire_pending()
    row = store.get("a1")
    assert row["state"] == "unexecuted"
    assert row["attempts"] == 0


def test_pending_past_deadline_never_claimed(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(0.1))
    time.sleep(0.15)
    assert claim(store) == []
    assert store.get("a1")["state"] == "unexecuted"


def test_finish_is_idempotent(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    claim(store)
    assert store.finish("a1", state="executed", receipt_status="succeeded")
    assert not store.finish("a1", state="failed", receipt_status="failed")
    assert store.get("a1")["state"] == "executed"


def test_duplicate_of_terminal_command_stays_terminal(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    claim(store)
    store.finish("a1", state="executed", receipt_status="succeeded")
    row, created = store.submit(
        device_id="d1", command_id="a1", deadline=deadline_in(60)
    )
    assert created is False and row["state"] == "executed"
    assert claim(store) == []


def test_backoff_prevents_tight_retry(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    assert len(claim(store, retry_seconds=5)) == 1
    store.release_to_pending("a1", "not delivered")
    assert claim(store, retry_seconds=5) == []
    assert claim(store)


# ---------------------------------------------------------- slot scheduling


def submit_many(store, device, prefix, count, seconds=600):
    for i in range(count):
        store.submit(
            device_id=device,
            command_id=f"{prefix}-{i}",
            deadline=deadline_in(seconds),
        )


def test_slot_cap_counts_in_flight(store):
    submit_many(store, "d1", "a", 3)
    submit_many(store, "d2", "b", 3)
    submit_many(store, "d3", "c", 3)
    deliveries, probes = store.claim_batch(
        retry_seconds=0, max_slots=4, in_flight=3, verify_limit=2
    )
    assert len(deliveries) + len(probes) == 1  # 4 - 3, not 4


def test_backlogged_device_cannot_starve_others(store):
    # d1 submitted first and has a deep backlog; d2/d3 arrive later.
    submit_many(store, "d1", "a", 50)
    submit_many(store, "d2", "b", 1)
    submit_many(store, "d3", "c", 1)

    seen = []
    for _ in range(3):
        deliveries, _ = store.claim_batch(
            retry_seconds=0, max_slots=4, in_flight=3, verify_limit=0
        )
        assert len(deliveries) == 1
        seen.append(deliveries[0]["device_id"])
        # The claimed command stays inflight, so its device is blocked next
        # round and the freed slot goes to another device's head.
    assert seen == ["d1", "d2", "d3"]


def test_verification_gets_a_bounded_share(store):
    # Two stuck inflight commands (both due) plus two queued devices.
    submit_many(store, "d1", "a", 1)
    submit_many(store, "d2", "b", 1)
    deliveries, _ = store.claim_batch(
        retry_seconds=0, max_slots=4, in_flight=0, verify_limit=0
    )
    assert [r["command_id"] for r in deliveries] == ["a-0", "b-0"]
    # Leases from the first claim are still in the future: nothing due yet.
    _, probes = store.claim_batch(
        retry_seconds=0, max_slots=4, in_flight=0, verify_limit=2
    )
    assert probes == []
    # Make both inflight commands due for verification.
    past = lease_expiry(-1)
    with store.connect() as conn:
        conn.execute(
            "UPDATE commands SET next_check_at=? WHERE state='inflight'", (past,)
        )
    submit_many(store, "d3", "c", 1)
    submit_many(store, "d4", "e", 1)
    deliveries, probes = store.claim_batch(
        retry_seconds=0, max_slots=4, in_flight=0, verify_limit=2
    )
    # Interleaved: one probe, one delivery, one probe, one delivery.
    assert [r["command_id"] for r in probes] == ["a-0", "b-0"]
    assert [r["command_id"] for r in deliveries] == ["c-0", "e-0"]
    # A reserved probe is not handed out again while its lease is fresh.
    deliveries2, probes2 = store.claim_batch(
        retry_seconds=0, max_slots=4, in_flight=0, verify_limit=2
    )
    assert deliveries2 == [] and probes2 == []


def test_claimed_delivery_carries_a_lease(store):
    submit_many(store, "d1", "a", 1)
    deliveries, _ = store.claim_batch(
        retry_seconds=0, max_slots=4, in_flight=0, verify_limit=0,
        lease_seconds=30,
    )
    row = deliveries[0]
    assert row["wire_state"] == "claimed"
    assert row["next_check_at"] is not None
    assert row["next_check_at"] > utcnow()
    # Not due for verification while the lease is fresh.
    assert store.inflight_due(now=utcnow(), limit=10) == []
    # Once the lease lapses the row can be probed again (crash recovery).
    assert store.inflight_due(now=lease_expiry(31), limit=10)


def test_inflight_due_is_bounded_and_ordered(store):
    submit_many(store, "d1", "a", 5)
    with store.connect() as conn:
        conn.execute(
            "UPDATE commands SET state='inflight', wire_state='sent', "
            "next_check_at=NULL WHERE state='pending'"
        )
    due = store.inflight_due(now=utcnow(), limit=2)
    assert [r["command_id"] for r in due] == ["a-0", "a-1"]


def test_scan_limit_keeps_lock_short_with_backlog(store):
    # Thousands of queued commands: one claim pass touches only a handful.
    submit_many(store, "d1", "a", 3000)
    start = time.monotonic()
    for _ in range(20):
        store.claim_batch(
            retry_seconds=0, max_slots=4, in_flight=4, verify_limit=2,
            scan_limit=64,
        )
    elapsed = time.monotonic() - start
    assert elapsed < 2.0, f"claim pass too slow with backlog: {elapsed:.3f}s"
