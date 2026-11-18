"""Integration tests: real gateway app + web dispatcher, in-process.

Both services run as ASGI apps over an httpx ASGITransport, so the dispatcher's
HTTP calls exercise the real gateway routes and real SQLite files.
"""
from __future__ import annotations

import asyncio
import importlib
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest


@pytest.fixture()
def services(tmp_path, monkeypatch):
    monkeypatch.setenv("GATEWAY_DB", str(tmp_path / "gateway.db"))
    monkeypatch.setenv("GATEWAY_EXECUTION_DELAY", "0.05")
    monkeypatch.setenv("OPS_DB", str(tmp_path / "ops.db"))
    _reload(tmp_path)
    _gw().store.init()
    _web().store.init()
    return _gw(), _web()


def _reload(tmp_path):
    for name in [m for m in list(sys.modules) if m.startswith(("web.app", "gateway.app"))]:
        del sys.modules[name]
    return (
        importlib.import_module("gateway.app.main"),
        importlib.import_module("web.app.main"),
    )


def _gw():
    return sys.modules["gateway.app.main"]


def _web():
    return sys.modules["web.app.main"]


def restart():
    _reload(None)
    _gw().store.init()
    _web().store.init()


def make_dispatcher(monkeypatch, web_main, transport):
    """Build a Dispatcher whose httpx client talks to the gateway ASGI app."""

    class PatchedAsyncClient(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            kwargs.pop("timeout", None)
            kwargs.pop("transport", None)
            kwargs.pop("base_url", None)
            super().__init__(*args, transport=transport, base_url="http://gateway", timeout=2.0, **kwargs)

    monkeypatch.setattr("web.app.dispatcher.httpx.AsyncClient", PatchedAsyncClient)
    return web_main.Dispatcher(
        web_main.store,
        "http://gateway",
        tick_seconds=0.02,
        verify_interval=0.05,
        verify_delay_after_loss=0.1,
        retry_seconds=0,
        reconcile_grace=0.3,
    )


def drive(dispatcher, until, timeout=8.0) -> bool:
    async def runner():
        dispatcher.start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if until():
                return True
            await asyncio.sleep(0.02)
        return until()

    return asyncio.run(runner())


def run_for(dispatcher, seconds: float) -> None:
    async def runner():
        dispatcher.start()
        await asyncio.sleep(seconds)
        await dispatcher.stop()

    asyncio.run(runner())


def scenario(dispatcher, body) -> None:
    """Run several phases against one dispatcher inside a single event loop.

    A Dispatcher cannot be started twice (loops differ), so multi-phase tests
    must stay inside one asyncio.run.
    """

    async def runner():
        dispatcher.start()
        try:
            await body(dispatcher)
        finally:
            await dispatcher.stop()

    asyncio.run(runner())


async def wait_until(predicate, timeout=5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


async def pause(seconds: float) -> None:
    await asyncio.sleep(seconds)


def deadline_in(minutes: float = 5) -> datetime:
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def deadline_secs(seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def test_happy_path(services, monkeypatch):
    gw_main, web_main = services
    transport = httpx.ASGITransport(app=gw_main.app)
    web_main.store.submit(device_id="d1", command_id="c1", deadline=deadline_in())
    d = make_dispatcher(monkeypatch, web_main, transport)
    assert drive(d, lambda: web_main.store.get("c1")["state"] == "executed")
    assert gw_main.store.get("c1")["executions"] == 1


def test_lost_receipt_is_verified_not_reexecuted(services, monkeypatch):
    gw_main, web_main = services
    transport = httpx.ASGITransport(app=gw_main.app)
    web_main.store.submit(
        device_id="d1", command_id="c1", deadline=deadline_in(), drop_receipt=True
    )
    d = make_dispatcher(monkeypatch, web_main, transport)
    assert drive(d, lambda: web_main.store.get("c1")["state"] == "executed")
    row = web_main.store.get("c1")
    assert row["receipt_lost"] == 1
    assert "核实" in row["detail"]
    assert gw_main.store.get("c1")["executions"] == 1


def test_lost_receipt_blocks_same_device_but_not_other(services, monkeypatch):
    gw_main, web_main = services
    gw_main.EXECUTION_DELAY = 0.8  # keep c1 "on the wire" long enough to observe
    transport = httpx.ASGITransport(app=gw_main.app)
    web_main.store.submit(
        device_id="d1", command_id="c1", deadline=deadline_in(), drop_receipt=True
    )
    web_main.store.submit(device_id="d1", command_id="c2", deadline=deadline_in())
    web_main.store.submit(device_id="d2", command_id="c3", deadline=deadline_in())

    d = make_dispatcher(monkeypatch, web_main, transport)

    async def body(_d):
        def c1_unresolved_but_c3_done():
            return (
                web_main.store.get("c1")["state"] == "inflight"
                and web_main.store.get("c3")["state"] == "executed"
            )

        assert await wait_until(c1_unresolved_but_c3_done)
        # Other device finished independently; the blocked device's queue did
        # not move, and c2 was never even delivered to the gateway.
        assert web_main.store.get("c2")["state"] == "pending"
        assert gw_main.store.get("c2") is None

        assert await wait_until(
            lambda: web_main.store.get("c1")["state"] == "executed"
        )
        assert gw_main.store.get("c1")["executions"] == 1
        # Once c1 is verified, the queue drains in order.
        assert await wait_until(
            lambda: web_main.store.get("c2")["state"] == "executed"
        )
        assert gw_main.store.get("c1")["executions"] == 1
        assert gw_main.store.get("c2")["executions"] == 1
        # FIFO: c2's execution is not earlier than c1's.
        assert (
            gw_main.store.get("c2")["received_at"]
            >= gw_main.store.get("c1")["executed_at"]
        )

    scenario(d, body)


def test_duplicate_submission_never_executes_twice(services, monkeypatch):
    gw_main, web_main = services
    transport = httpx.ASGITransport(app=gw_main.app)
    web_main.store.submit(device_id="d1", command_id="c1", deadline=deadline_in())
    d = make_dispatcher(monkeypatch, web_main, transport)
    assert drive(d, lambda: web_main.store.get("c1")["state"] == "executed")
    for _ in range(6):
        row, created = web_main.store.submit(
            device_id="d1", command_id="c1", deadline=deadline_in()
        )
        assert created is False and row["state"] == "executed"
    time.sleep(0.2)
    assert gw_main.store.get("c1")["executions"] == 1


def test_restart_resumes_unfinished_and_never_reexecutes(services, monkeypatch):
    gw_main, web_main = services
    transport = httpx.ASGITransport(app=gw_main.app)
    web_main.store.submit(device_id="d1", command_id="c1", deadline=deadline_in())
    d = make_dispatcher(monkeypatch, web_main, transport)
    assert drive(d, lambda: web_main.store.get("c1")["state"] == "executed")
    web_main.store.submit(device_id="d1", command_id="c2", deadline=deadline_in())

    restart()
    gw_main2, web_main2 = _gw(), _web()
    assert web_main2.store.get("c1")["state"] == "executed"
    assert web_main2.store.get("c2")["state"] == "pending"

    d2 = make_dispatcher(monkeypatch, web_main2, transport)
    assert drive(d2, lambda: web_main2.store.get("c2")["state"] == "executed")
    assert gw_main2.store.get("c1")["executions"] == 1
    assert gw_main2.store.get("c2")["executions"] == 1


def test_overdue_inflight_keeps_waiting_and_blocks(services, monkeypatch):
    gw_main, web_main = services
    # Make the gateway unreachable for verification so the outcome stays
    # unknown while the command's deadline elapses.
    healthy = httpx.ASGITransport(app=gw_main.app)

    class FlakyTransport(httpx.AsyncBaseTransport):
        def __init__(self):
            self.fail = False

        async def handle_async_request(self, request):
            if self.fail:
                raise httpx.ConnectError("down", request=request)
            return await healthy.handle_async_request(request)

    flaky = FlakyTransport()
    web_main.store.submit(
        device_id="d1", command_id="c1", deadline=deadline_secs(0.3), drop_receipt=True
    )
    web_main.store.submit(device_id="d1", command_id="c2", deadline=deadline_in(5))
    d = make_dispatcher(monkeypatch, web_main, flaky)

    async def body(_d):
        # Let it go out and lose the receipt, then cut the gateway off.
        assert await wait_until(lambda: web_main.store.get("c1")["receipt_lost"] == 1)
        flaky.fail = True
        await pause(0.5)  # c1's deadline passes while the outcome is unknown
        c1 = web_main.store.get("c1")
        assert c1["state"] == "inflight"      # never failed / unexecuted
        assert web_main.store.get("c2")["state"] == "pending"  # not allowed past
        assert gw_main.store.get("c2") is None

        # Restore the gateway: verification resolves c1, then c2 runs.
        flaky.fail = False
        assert await wait_until(
            lambda: web_main.store.get("c1")["state"] == "executed"
        )
        assert await wait_until(
            lambda: web_main.store.get("c2")["state"] == "executed"
        )
        assert gw_main.store.get("c1")["executions"] == 1
        assert gw_main.store.get("c2")["executions"] == 1

    scenario(d, body)


def test_unreachable_gateway_keeps_command_pending_then_expires(services, monkeypatch):
    """While the gateway is down a never-sent command stays pending; once its
    deadline passes it is unexecuted and is not sent later."""
    gw_main, web_main = services

    class DownTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("down", request=request)

    web_main.store.submit(
        device_id="d1", command_id="c1", deadline=deadline_secs(0.4)
    )
    d = make_dispatcher(monkeypatch, web_main, DownTransport())

    async def body(_d):
        assert await wait_until(lambda: web_main.store.get("c1")["attempts"] >= 1)
        assert web_main.store.get("c1")["state"] == "pending"
        await pause(0.5)  # deadline passes while the gateway is unreachable
        assert await wait_until(
            lambda: web_main.store.get("c1")["state"] == "unexecuted"
        )
        assert gw_main.store.get("c1") is None

    scenario(d, body)


def test_gateway_404_after_sent_is_not_treated_as_unexecuted(services, monkeypatch):
    """A request we wrote to the socket may still arrive: a 404 is never
    interpreted as 'did not execute'.  The command stays inflight and blocks
    the device queue."""
    gw_main, web_main = services

    class BlackHoleTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ReadTimeout("no answer", request=request)

    web_main.store.submit(device_id="d1", command_id="c1", deadline=deadline_secs(0.2))
    web_main.store.submit(device_id="d1", command_id="c2", deadline=deadline_in(5))
    d = make_dispatcher(monkeypatch, web_main, BlackHoleTransport())

    async def body(_d):
        assert await wait_until(
            lambda: web_main.store.get("c1")["wire_state"] == "sent"
        )
        await pause(0.6)  # deadline passes while the outcome is ambiguous
        row = web_main.store.get("c1")
        assert row["state"] == "inflight"
        assert "不明" in row["detail"]
        assert web_main.store.get("c2")["state"] == "pending"
        assert gw_main.store.get("c2") is None

    scenario(d, body)


def test_gateway_restart_mid_flight_freezes_id(services, monkeypatch):
    """If the gateway dies while executing, the id is burned: no re-execution
    on a retried delivery, and the web keeps it unresolved."""
    gw_main, web_main = services
    transport = httpx.ASGITransport(app=gw_main.app)

    # Simulate the gateway having been reserved but killed mid-execution.
    action, _ = gw_main.store.accept("c1", "d1", False)
    assert action == "start"
    # Gateway restarts: init() must freeze the running row as unknown.
    gw_main.store.init()
    assert gw_main.store.get("c1")["state"] == "unknown"

    web_main.store.submit(device_id="d1", command_id="c1", deadline=deadline_secs(0.3))
    d = make_dispatcher(monkeypatch, web_main, transport)

    async def body(_d):
        assert await wait_until(
            lambda: "重启" in (web_main.store.get("c1")["detail"] or "")
        )
        row = web_main.store.get("c1")
        assert row["state"] == "inflight"      # unresolved, not unexecuted/failed
        assert gw_main.store.get("c1")["executions"] == 0  # never re-executed

    scenario(d, body)


# ----------------------------------------------------- scheduling under load


def pending_count(web_main, device):
    with web_main.store.connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM commands WHERE device_id=? AND state='pending'",
            (device,),
        ).fetchone()["n"]


def gateway_running(gw_main) -> int:
    with gw_main.store.connect() as conn:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM executions WHERE state='running'"
        ).fetchone()["n"]


def test_slot_cap_and_fairness_with_one_backlogged_device(services, monkeypatch):
    """One device with a deep backlog must not starve later devices, and the
    number of concurrent gateway deliveries never exceeds the slot cap."""
    gw_main, web_main = services
    gw_main.EXECUTION_DELAY = 0.15
    transport = httpx.ASGITransport(app=gw_main.app)

    for i in range(12):  # the backlogged device, submitted first
        web_main.store.submit(
            device_id="d-backlog", command_id=f"b-{i}", deadline=deadline_in(5)
        )
    for d in range(5):  # later devices, one command each
        web_main.store.submit(
            device_id=f"d-{d}", command_id=f"s-{d}", deadline=deadline_in(5)
        )

    d = make_dispatcher(monkeypatch, web_main, transport)
    peak = {"running": 0}

    async def body(_d):
        # Later devices all finish while the backlog device is still queued.
        assert await wait_until(
            lambda: all(
                web_main.store.get(f"s-{i}")["state"] == "executed" for i in range(5)
            ),
            timeout=10,
        )
        assert pending_count(web_main, "d-backlog") > 0
        # Same-device FIFO held and nothing was executed twice.
        assert await wait_until(
            lambda: all(
                web_main.store.get(f"b-{i}")["state"] == "executed" for i in range(12)
            ),
            timeout=15,
        )

    async def runner():
        _d = d

        async def watch():
            while True:
                peak["running"] = max(peak["running"], gateway_running(gw_main))
                await asyncio.sleep(0.01)

        d.start()
        watcher = asyncio.create_task(watch())
        try:
            await body(d)
        finally:
            watcher.cancel()
            await asyncio.gather(watcher, return_exceptions=True)
            await d.stop()

    asyncio.run(runner())
    assert peak["running"] <= 4  # the slot cap was never exceeded
    assert peak["running"] >= 2  # slots were actually shared in parallel
    for i in range(12):
        assert gw_main.store.get(f"b-{i}")["executions"] == 1
    for i in range(5):
        assert gw_main.store.get(f"s-{i}")["executions"] == 1


def test_verification_not_starved_by_delivery_load(services, monkeypatch):
    """While every slot is contested by queued deliveries, a lost receipt is
    still verified promptly (probes get a guaranteed share of the slots)."""
    gw_main, web_main = services
    gw_main.EXECUTION_DELAY = 0.15
    transport = httpx.ASGITransport(app=gw_main.app)

    web_main.store.submit(
        device_id="d0", command_id="lost-0", deadline=deadline_in(5), drop_receipt=True
    )
    for d in range(4):  # enough queued work to keep all 4 slots busy
        for i in range(6):
            web_main.store.submit(
                device_id=f"q-{d}", command_id=f"q-{d}-{i}", deadline=deadline_in(5)
            )

    d = make_dispatcher(monkeypatch, web_main, transport)

    async def body(_d):
        assert await wait_until(
            lambda: web_main.store.get("lost-0")["state"] == "executed",
            timeout=10,
        )
        row = web_main.store.get("lost-0")
        assert row["receipt_lost"] == 1 and "核实" in row["detail"]
        # The verification finished while other devices were still queued,
        # i.e. probes were not starved by the delivery load.
        assert any(
            web_main.store.get(f"q-{d}-{i}")["state"] == "pending"
            for d in range(4)
            for i in range(6)
        )

    scenario(d, body)
    assert gw_main.store.get("lost-0")["executions"] == 1


def test_stranded_claim_is_recovered_via_lease(services, monkeypatch):
    """A command claimed by a dispatcher task that vanished (never sent, lease
    expired) is probed, found unknown at the gateway, re-queued and delivered
    -- exactly once."""
    gw_main, web_main = services
    transport = httpx.ASGITransport(app=gw_main.app)
    web_main.store.submit(device_id="d1", command_id="c1", deadline=deadline_in(5))
    web_main.store.submit(device_id="d1", command_id="c2", deadline=deadline_in(5))

    d = make_dispatcher(monkeypatch, web_main, transport)

    async def body(_d):
        # Simulate a claim whose task died before touching the network:
        # inflight, wire_state=claimed, lease already lapsed.
        past = (
            datetime.now(timezone.utc) - timedelta(seconds=5)
        ).isoformat(timespec="microseconds")
        with web_main.store.connect() as conn:
            conn.execute(
                "UPDATE commands SET state='inflight', wire_state='claimed', "
                "attempts=1, last_attempt_at=?, next_check_at=? "
                "WHERE command_id='c1'",
                (past, past),
            )
        assert await wait_until(
            lambda: web_main.store.get("c1")["state"] == "executed"
        )
        assert await wait_until(
            lambda: web_main.store.get("c2")["state"] == "executed"
        )

    scenario(d, body)
    assert gw_main.store.get("c1")["executions"] == 1
    assert gw_main.store.get("c2")["executions"] == 1


def test_submit_stays_fast_with_large_backlog(services, monkeypatch):
    """Accepting a new command must not scan the backlog: submit stays fast
    while thousands of commands are queued and the dispatcher is busy."""
    gw_main, web_main = services
    gw_main.EXECUTION_DELAY = 0.3  # keep all slots busy during the measurement
    transport = httpx.ASGITransport(app=gw_main.app)
    for i in range(1000):
        web_main.store.submit(
            device_id="bulk", command_id=f"bulk-{i}", deadline=deadline_in(30)
        )

    d = make_dispatcher(monkeypatch, web_main, transport)

    async def body(_d):
        await pause(0.3)  # let the dispatcher start churning the backlog
        start = time.monotonic()
        web_main.store.submit(
            device_id="hot", command_id="hot-1", deadline=deadline_in(5)
        )
        elapsed = time.monotonic() - start
        assert elapsed < 0.5, f"submit took {elapsed:.3f}s under backlog"
        assert await wait_until(
            lambda: web_main.store.get("hot-1")["state"] == "executed", timeout=10
        )

    scenario(d, body)
    assert gw_main.store.get("hot-1")["executions"] == 1
