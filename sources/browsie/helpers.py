"""Browsie pipeline helpers"""
from dlt.common.typing import StrAny, DictStrAny, TDataItem
from dlt.sources.helpers import requests
import time
from .settings import API_URL, API_KEY, DEFAULT_DEADLINE, DEFAULT_INTERVAL

queued = "queued"  # -> started
started = "started"  # -> succeeded or failed
succeeded = "succeeded"
failed = "failed"


def _authorized_request(path: str, data: DictStrAny = None) -> DictStrAny:
    r = requests.request(
        "POST" if data else "GET",
        API_URL + path,
        headers={"APIkey": API_KEY, "Content-Type": "application/json; charset=utf-8"},
        json=data,
    )
    return r.json()


def _start_job(data: DictStrAny) -> TDataItem:
    return _authorized_request("/job", data)


def _check_job(job_id: StrAny) -> TDataItem:
    return _authorized_request(f"/job/{job_id}")


def _wait_for_job(job_id: StrAny, interval: int = DEFAULT_INTERVAL) -> TDataItem:
    deadline = time.time() + DEFAULT_DEADLINE
    waiting_for_queue = True
    status = "queued"
    while True:
        time.sleep(interval)
        jr = _check_job(job_id)
        if jr["status"] != status:
            print(f"job {job_id} is now in state {jr['status']}")
            if waiting_for_queue and jr["status"] == "started":
                waiting_for_queue = False
                deadline = time.time() + 120  # self.job_spec.timeout_ms / 1000
            status = jr["status"]
        if time.time() > deadline or jr["status"] in ("failed", "succeeded"):
            return jr


def run_job(data: DictStrAny) -> TDataItem:
    jr = _start_job(data)
    return _wait_for_job(jr["id"])
