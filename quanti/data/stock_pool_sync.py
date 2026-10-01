"""Dashboard Tushare roster sync: one request every hour and one minute."""

from __future__ import annotations

import copy
import os
import threading
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

INTERVAL_SECONDS = 3660
_JOB_POOL = "_stock_pool_sync"
_TZ = ZoneInfo("Asia/Shanghai")
_ROSTERS = (("L", "上市股票"), ("D", "退市股票"), ("P", "暂停上市股票"))


def _now() -> datetime:
    return datetime.now(_TZ)


class StockPoolSync:
    """One task per server, persisted in the existing sync_jobs table.

    A dedicated thread waits without holding a DB lock or an HTTP request.
    Restarted servers fail interrupted tasks instead of resuming requests.
    """

    def __init__(self, db) -> None:
        self._db = db
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        row = db.conn.execute(
            "SELECT job_id FROM sync_jobs WHERE pool_name=? ORDER BY rowid DESC LIMIT 1",
            (_JOB_POOL,)).fetchone()
        job = db.get_sync_job(row[0]) if row else None
        self._state = (job["warnings"].get("stock_pool") if job else None) or {
            "job_id": None, "source": None, "status": "idle", "active": False,
            "synced": 0, "error": "", "last_requested_at": None, "stages": [],
        }

    def status(self) -> dict:
        with self._lock:
            return copy.deepcopy(self._state)

    def recover(self) -> None:
        with self._lock:
            if self._state["active"]:
                self._fail("服务重启，股票池同步已中断；已写入的数据保留")

    def start(self, adapter) -> dict:
        with self._lock:
            if self._state["active"]:
                return self.status()
            now = _now()
            last = self._state.get("last_requested_at")
            first = max(now, datetime.fromisoformat(last) + timedelta(
                seconds=INTERVAL_SECONDS)) if last else now
            self._state = {
                "job_id": f"stock_{uuid.uuid4().hex}", "source": "tushare",
                "status": "waiting", "active": True, "synced": 0, "error": "",
                "last_requested_at": last,
                "stages": [
                    {"list_status": status, "label": label, "status": "pending",
                     "planned_at": (first + timedelta(
                         seconds=i * INTERVAL_SECONDS)).isoformat(),
                     "requested_at": None, "finished_at": None,
                     "count": 0, "error": ""}
                    for i, (status, label) in enumerate(_ROSTERS)
                ],
            }
            self._db.create_sync_job(self._state["job_id"], _JOB_POOL, 3)
            self._save()
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, args=(adapter,), name="stock-pool-sync", daemon=True)
            self._thread.start()
            return self.status()

    def _save(self) -> None:
        completed = sum(s["status"] == "success" for s in self._state["stages"])
        status = "running" if self._state["active"] else self._state["status"]
        errors = {"stock_pool": self._state["error"]} if self._state["error"] else {}
        self._db.update_sync_job(
            self._state["job_id"], completed, status, errors,
            {"stock_pool": self._state})

    def _fail(self, message: str, index: int | None = None) -> None:
        self._state.update(status="error", active=False, error=message)
        for i, stage in enumerate(self._state["stages"]):
            if i == index or stage["status"] == "running":
                stage.update(status="error", error=message, finished_at=_now().isoformat())
            elif stage["status"] == "pending":
                stage["status"] = "skipped"
        self._save()

    def _requested(self, index: int) -> None:
        with self._lock:
            if self._stop.is_set():
                raise InterruptedError("服务停止，股票池同步已中断")
            now = _now()
            self._state["status"] = "running"
            self._state["last_requested_at"] = now.isoformat()
            self._state["stages"][index].update(
                status="running", requested_at=now.isoformat())
            for i in range(index + 1, 3):
                self._state["stages"][i]["planned_at"] = (
                    now + timedelta(seconds=(i - index) * INTERVAL_SECONDS)).isoformat()
            # Persist even failed attempts, so a new task observes the cooldown.
            self._save()

    def _run(self, adapter) -> None:
        index = 0
        try:
            for index, (status, _) in enumerate(_ROSTERS):
                while True:
                    with self._lock:
                        planned = datetime.fromisoformat(
                            self._state["stages"][index]["planned_at"])
                        last = self._state["last_requested_at"]
                        if last:
                            planned = max(planned, datetime.fromisoformat(last) + timedelta(
                                seconds=INTERVAL_SECONDS))
                        delay = max(0, (planned - _now()).total_seconds())
                    if self._stop.wait(delay):
                        raise InterruptedError("服务停止，股票池同步已中断；已写入的数据保留")
                    # Recheck the wall clock after waiting before issuing a request.
                    if _now() >= planned:
                        break
                count = adapter.sync_stock_list_status(
                    status, on_request=lambda: self._requested(index))
                with self._lock:
                    self._state["stages"][index].update(
                        status="success", count=count, finished_at=_now().isoformat())
                    self._state["synced"] += count
                    if self._stop.is_set() and index < 2:
                        self._fail("服务停止，股票池同步已中断；已写入的数据保留")
                        return
                    self._state["status"] = "done" if index == 2 else "waiting"
                    self._state["active"] = index != 2
                    self._save()
        except Exception as exc:  # noqa: BLE001 - surface one failure; never retry
            token = getattr(adapter, "_token", None) or os.environ.get("TUSHARE_TOKEN", "")
            message = str(exc)
            if token:
                message = message.replace(token, "[已隐藏]")
            with self._lock:
                self._fail(message, index)

    def shutdown(self) -> None:
        self._stop.set()
        # A request in progress finishes before shutdown; no later request starts.
        if self._thread is not None:
            self._thread.join()
