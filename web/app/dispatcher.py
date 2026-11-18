"""Background delivery / verification worker for the operations web.

Safety rules encoded here:

* A command is only ever *sent* while it is ``pending``.  Once it is
  ``inflight`` it is only ever *queried* by command id -- a missing receipt is
  never interpreted as failure and never triggers a second execution.
* The only exception is a definitive "unknown command" (404) answer from the
  gateway, which proves the request never arrived; then, and only then, the
  command may be re-queued (at-most-once still holds at the gateway).
* Per device, one inflight command blocks every younger command, including
  after its deadline.  Other devices are unaffected.

Scheduling rules (the point of this module):

* At most ``max_slots`` gateway interactions (deliveries + verification
  probes) run concurrently; everything else stays queued in SQLite.
* Freed slots are shared fairly: verification probes and device queue heads
  alternate, and device heads are visited round-robin, so a device with a
  huge backlog cannot starve later devices and probes cannot be starved.
* Picking work never scans the whole backlog: the store reads only small
  bounded candidate pools inside one short write transaction.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import httpx

from .store import Store, is_overdue

log = logging.getLogger("ops.dispatcher")


class Dispatcher:
    def __init__(
        self,
        store: Store,
        gateway_url: str,
        *,
        tick_seconds: float = 0.25,
        verify_interval: float = 1.0,
        verify_delay_after_loss: float = 3.0,
        http_timeout: float = 2.0,
        retry_seconds: float = 1.0,
        reconcile_grace: float = 5.0,
        max_slots: int = 4,
        scan_limit: int = 200,
    ):
        self.store = store
        self.gateway_url = gateway_url.rstrip("/")
        self.tick_seconds = tick_seconds
        self.verify_interval = verify_interval
        self.verify_delay_after_loss = verify_delay_after_loss
        self.http_timeout = http_timeout
        self.retry_seconds = retry_seconds
        # A 404 can race with a request that is still travelling to the
        # gateway; require this much quiet time after the last attempt before
        # trusting "never received".
        self.reconcile_grace = reconcile_grace
        # Total gateway interactions (delivery + verification) in flight.
        self.max_slots = max(1, max_slots)
        # Upper bound for the per-tick candidate reads; also caps how many
        # slots one tick may spend on verification probes so deliveries of
        # other devices always get a share.
        self.scan_limit = max(8, scan_limit)
        self.verify_share = max(1, min(self.max_slots - 1, self.max_slots // 2))
        # A claimed delivery must reach the network (or be released) within
        # this lease; afterwards the row may be probed by command id again.
        self.slot_lease_seconds = max(
            5.0, 3 * self.http_timeout + 2 * self.tick_seconds + 5.0
        )
        self._tasks: dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()
        self._loop_task: asyncio.Task | None = None

    # ------------------------------------------------------------- lifecycle
    def start(self) -> None:
        # A restart may have interrupted a claim before it was written out.
        # Those can safely re-queue; anything already sent stays inflight and
        # will be resolved by querying the gateway.  Stale leases from before
        # the restart are dropped so sent rows are probed right away.
        self.store.reset_claimed_to_pending()
        self.store.make_inflight_due()
        self._loop_task = asyncio.create_task(self._run(), name="dispatcher")

    async def stop(self) -> None:
        self._stop.set()
        if self._loop_task:
            self._loop_task.cancel()
        for task in list(self._tasks.values()):
            task.cancel()
        await asyncio.gather(
            *([self._loop_task] if self._loop_task else []),
            *self._tasks.values(),
            return_exceptions=True,
        )

    # -------------------------------------------------------------- main loop
    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.store.expire_pending()
                # Prune finished ids so the in-flight count stays exact.
                for cid in [c for c, t in self._tasks.items() if t.done()]:
                    self._tasks.pop(cid, None)
                deliveries, probes = self.store.claim_batch(
                    retry_seconds=self.retry_seconds,
                    max_slots=self.max_slots,
                    in_flight=len(self._tasks),
                    scan_limit=self.scan_limit,
                    verify_limit=self.verify_share,
                    verify_interval=self.verify_interval,
                    lease_seconds=self.slot_lease_seconds,
                )
                for row in deliveries:
                    self._spawn(row["command_id"], self._deliver(row["command_id"]))
                for row in probes:
                    self._spawn(row["command_id"], self._verify(row["command_id"]))
            except Exception:  # keep the worker alive across transient errors
                log.exception("dispatcher tick failed")
            await asyncio.sleep(self.tick_seconds)

    def _spawn(self, command_id: str, coro) -> None:
        if command_id in self._tasks:
            coro.close()
            return
        task = asyncio.create_task(coro, name=f"cmd:{command_id}")
        self._tasks[command_id] = task
        task.add_done_callback(lambda _t, cid=command_id: self._tasks.pop(cid, None))

    # --------------------------------------------------------------- delivery
    async def _deliver(self, command_id: str) -> None:
        row = self.store.get(command_id)
        if row is None or row["state"] != "inflight":
            return
        payload = {
            "command_id": command_id,
            "device_id": row["device_id"],
            "drop_receipt": bool(row["drop_receipt"]),
        }
        self.store.mark_sent(command_id)
        try:
            async with httpx.AsyncClient(timeout=self.http_timeout) as client:
                resp = await client.post(
                    f"{self.gateway_url}/api/commands", json=payload
                )
        except (httpx.ConnectError, httpx.ConnectTimeout):
            # A failed *connect* means no request bytes were written, so this
            # is definitively "not executed".  Safe to re-queue.
            self.store.release_to_pending(command_id, "网关不可达，未发出，等待重试")
            return
        except httpx.HTTPError as exc:
            # The request may have arrived.  Outcome unknown: verify by id.
            # Hand the row to the verification cadence; the slot is released.
            self.store.defer_check(command_id, self.verify_interval)
            self.store.mark_unverified(command_id, f"下发结果不明（{type(exc).__name__}），待核实")
            return

        self._handle_delivery_response(command_id, resp)

    def _handle_delivery_response(self, command_id: str, resp: httpx.Response) -> None:
        if resp.status_code == 200:
            body = resp.json()
            status = body.get("status")
            gw_state = body.get("state")
            if gw_state == "executed" and status in ("succeeded", "failed"):
                self.store.finish(
                    command_id,
                    state="executed" if status == "succeeded" else "failed",
                    receipt_status=status,
                    detail="网关回执：执行成功" if status == "succeeded" else "网关回执：执行失败",
                )
                return
            if gw_state == "unknown":
                # The id was burned by a gateway restart mid-execution.  A 200
                # here is NOT an execution receipt.
                self.store.mark_unverified(
                    command_id,
                    "网关重启导致该指令结果不明；不重发、不越过，需人工核实",
                )
                return
            # Any other 200 without a concrete outcome is still ambiguous.
            self.store.defer_check(command_id, self.verify_interval)
            self.store.mark_unverified(
                command_id, f"网关回执状态 {gw_state!r}/{status!r} 不明确，待核实"
            )
            return
        if resp.status_code == 202:
            # The gateway committed the execution but the receipt was lost on
            # the way back.  Keep inflight and verify by command id later.
            self.store.mark_receipt_lost(
                command_id, "回执丢失，已执行与否需按指令号核实"
            )
            # Give the gateway a moment before the first probe; the delay is
            # stored on the row so it survives a restart.
            self.store.defer_check(command_id, self.verify_delay_after_loss)
            return
        if resp.status_code == 409 and "different device" in resp.text:
            self.store.finish(
                command_id,
                state="rejected",
                detail="网关拒绝：指令号已属于其他设备",
            )
            return
        if resp.status_code == 409:
            # Gateway is still running this id; we must wait, not retry blindly.
            self.store.defer_check(command_id, self.verify_interval)
            self.store.mark_unverified(command_id, "网关仍在执行该指令，待核实")
            return
        # 5xx or anything unexpected: we cannot tell, so verify by id.
        self.store.defer_check(command_id, self.verify_interval)
        self.store.mark_unverified(
            command_id, f"网关异常响应 HTTP {resp.status_code}，待核实"
        )

    # ----------------------------------------------------------- verification
    async def _verify(self, command_id: str) -> None:
        row = self.store.get(command_id)
        if row is None or row["state"] != "inflight":
            return
        try:
            async with httpx.AsyncClient(timeout=self.http_timeout) as client:
                resp = await client.get(
                    f"{self.gateway_url}/api/commands/{command_id}"
                )
        except httpx.HTTPError as exc:
            self.store.mark_unverified(
                command_id, f"核实失败（{type(exc).__name__}），结果仍不明"
            )
            return

        if resp.status_code == 200:
            body = resp.json()
            state = body.get("state")
            if state == "executed":
                status = body.get("status")
                self.store.finish(
                    command_id,
                    state="executed" if status == "succeeded" else "failed",
                    receipt_status=status,
                    detail=f"按指令号核实：网关已{'成功' if status == 'succeeded' else '失败'}执行"
                    f"（执行次数 {body.get('executions')}）",
                )
            elif state == "unknown":
                # Gateway restarted mid-execution: the outcome is genuinely
                # unknown.  Never resend, never pass it; wait for an operator.
                self.store.mark_unverified(
                    command_id,
                    "网关执行途中重启，结果不明；不自动重发，需人工核实",
                )
            elif state == "running":
                self.store.mark_unverified(command_id, "网关核实：仍在执行，继续等待")
            else:
                self.store.mark_unverified(command_id, f"网关核实：状态 {state}，再等待")
            return

        if resp.status_code == 404:
            # The gateway has no record -- but a request that is still in
            # flight could yet arrive.  Only trust it after a quiet period
            # measured from the last delivery claim (`last_attempt_at` is
            # never touched by verification probes, so it cannot be
            # refreshed into hiding the quiet time).
            last = row["last_attempt_at"] or row["created_at"]
            elapsed = (
                datetime.now(timezone.utc) - datetime.fromisoformat(last)
            ).total_seconds()
            if elapsed < self.reconcile_grace:
                self.store.mark_unverified(
                    command_id,
                    "网关暂无该指令记录，等待静默期后再次核实",
                )
                return
            if row["wire_state"] != "claimed":
                # It was written to the socket.  Even after the grace period,
                # "gateway has no record" is not proof it never ran: the
                # request could still surface.  Keep waiting, never pass it.
                self.store.mark_unverified(
                    command_id,
                    "已发出但网关无记录，结果未明；不重发、不越过，需人工核实",
                )
                return
            self._resolve_never_executed(command_id)
            return

        self.store.mark_unverified(
            command_id, f"核实返回 HTTP {resp.status_code}，结果仍不明"
        )

    def _resolve_never_executed(self, command_id: str) -> None:
        """Only called for commands proven never to have been written out."""
        row = self.store.get(command_id)
        if row is None or row["state"] != "inflight":
            return
        if is_overdue(row["deadline"]):
            self.store.finish(
                command_id,
                state="unexecuted",
                detail="核实确认网关未收到，且已过截止时间，未执行",
            )
        else:
            self.store.release_to_pending(
                command_id, "核实确认网关未收到，重新排队下发"
            )
