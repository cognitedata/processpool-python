import os
import signal
import time
import traceback
from multiprocessing import process
from typing import Any, List, Tuple

import pytest

from cognite.processpool import ProcessPool, ProcessPoolShutDownException, WorkerDiedException
from cognite.processpool.processpool import JobFailedException


class ExceptionWithLambda(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
        self._unwrap_fn = lambda x: x


class AuthHTTPError(Exception):
    def __init__(self, reason: str, response: str):
        self.reason = reason
        self.response = response

    def __str__(self) -> str:
        return f"HTTP error in authentication. {self.reason}"


class BadObjectReturner:
    def run(self, *args: Tuple[Any, ...]) -> List[Any]:
        bad_list: List[Any] = []
        for _ in range(10000):
            bad_list = [0, bad_list]
        return bad_list


class CrashingSquareNumberWorker:
    def run(self, msg: str, num: float, *args: Tuple[Any, ...]) -> float:
        time.sleep(0.1)
        if num == 1337:
            pid = process.current_process().pid
            assert pid is not None
            os.kill(pid, signal.SIGKILL)
        if num == 13:
            raise ValueError("Got unlucky!")
        return num * num


class ExceptionLambdaWorker:
    def run(self) -> None:
        raise ExceptionWithLambda("annoying")


def test_plain_exception() -> None:
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 13)
    with pytest.raises(JobFailedException) as excinfo:
        f1.result()
    assert "ValueError" == str(excinfo.value.original_exception_type)
    assert "JobFailedException: ValueError(Got unlucky!)" == str(excinfo.value)
    pool.join()
    assert pool.terminated


def test_exception_lambda() -> None:
    pool = ProcessPool(ExceptionLambdaWorker, 1)
    f1 = pool.submit_job()
    with pytest.raises(Exception) as excinfo:
        f1.result()
    assert "JobFailedException: ExceptionWithLambda(annoying)" == str(excinfo.value)
    pool.join()
    assert pool.terminated


def test_exception_traceback() -> None:
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 13)
    exc = f1.exception()
    assert exc is not None
    exc_traceback = "".join(traceback.TracebackException.from_exception(exc).format())
    assert "Traceback (most recent call last)" in exc_traceback
    pool.join()
    assert pool.terminated


def test_submit_or_join_after_terminate() -> None:
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 0)
    pool.terminate()
    with pytest.raises(WorkerDiedException) as excinfo:
        f1.result()
    assert isinstance(excinfo.value.code, int)
    assert excinfo.value.code < 0
    with pytest.raises(ProcessPoolShutDownException):
        pool.submit_job("transform", 13)
    with pytest.raises(ProcessPoolShutDownException):
        pool.join()
    assert pool.terminated


def test_run_job_exception() -> None:
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    assert 16 == pool.run_job("transform", 4)
    with pytest.raises(JobFailedException):
        pool.run_job("transform", 13)
    pool.join()
    assert pool.terminated


def test_pickle_bad_arg_returned() -> None:
    pool = ProcessPool(BadObjectReturner, 1)
    f1 = pool.submit_job(":(")
    with pytest.raises(Exception) as excinfo:  # not working
        f1.result()
    assert "maximum recursion depth exceeded while pickling" in str(excinfo.value)
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated


def test_pickle_bad_arg_given() -> None:
    bad_arg: List[Any] = []
    for _ in range(10000):
        bad_arg = [0, bad_arg]

    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job(bad_arg)
    with pytest.raises(Exception) as excinfo:  # not working
        f1.result()
    assert "maximum recursion depth exceeded while pickling" in str(excinfo.value)
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated


def test_crash() -> None:
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", num=1)
    f2 = pool.submit_job("transform", 1337)
    f3 = pool.submit_job("transform", 3)
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated
    assert 1 == f1.result()
    assert 9 == f3.result()
    with pytest.raises(WorkerDiedException):
        f2.result()
