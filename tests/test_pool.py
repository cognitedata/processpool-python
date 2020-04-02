import time
import traceback

import pytest

from cognite.processpool import ProcessPool, ProcessPoolShutDownException, WorkerDiedException


class SquareNumberWorker:
    def run(self, msg, num, *args):
        return num * num


class CrashingSquareNumberWorker:
    def run(self, msg, num, *args):
        time.sleep(0.1)
        if num == 1337:
            a = []
            while True:
                a.extend([0] * 1000000)
        if num == 13:
            raise ValueError("Got unlucky!")
        return num * num


def test_simple():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 2)
    f2 = pool.submit_job("transform", 3)
    assert 4 == f1.result()
    assert 9 == f2.result()
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated


def test_crash():
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


def test_exception():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 13)
    with pytest.raises(ValueError) as excinfo:
        f1.result()
    assert "unlucky" in str(excinfo.value)
    pool.join()
    assert pool.terminated


def test_exception_traceback():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 13)
    exc = f1.exception()
    assert exc is not None
    exc_traceback = "".join(traceback.TracebackException.from_exception(exc).format())
    assert " line 22, in run" in exc_traceback
    pool.join()
    assert pool.terminated


def test_run_job():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    assert 16 == pool.run_job("transform", 4)
    with pytest.raises(ValueError):
        pool.run_job("transform", 13)
    pool.join()
    assert pool.terminated


def test_submit_or_join_after_join():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 0)
    assert 0 == f1.result()
    pool.join()
    assert pool.terminated
    with pytest.raises(ProcessPoolShutDownException):
        pool.submit_job("transform", 13)
    with pytest.raises(ProcessPoolShutDownException):
        pool.join()
    assert pool.terminated


def test_submit_or_join_after_terminate():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 0)
    pool.terminate()
    with pytest.raises(WorkerDiedException) as excinfo:
        f1.result()
    assert excinfo.value.code < 0
    with pytest.raises(ProcessPoolShutDownException):
        pool.submit_job("transform", 13)
    with pytest.raises(ProcessPoolShutDownException):
        pool.join()
    assert pool.terminated


def test_many():
    class SquareNumberWorker:
        def run(self, num, *args):
            return num * num

    pool = ProcessPool(SquareNumberWorker, 4)
    futures = [pool.submit_job(i) for i in range(100)]
    result = [f.result() for f in futures]
    assert [n * n for n in range(100)] == result
    pool.join()
