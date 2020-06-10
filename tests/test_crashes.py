import time
import traceback

import pytest

from cognite.processpool import ProcessPool, ProcessPoolShutDownException, WorkerDiedException


class ExceptionWithLambda(Exception):
    def __init__(self, msg):
        super().__init__(msg)
        self._unwrap_fn = lambda x: x


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


class ExceptionLambdaWorker:
    def run(self):
        raise ExceptionWithLambda("annoying")


def test_exception():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 13)
    with pytest.raises(ValueError) as excinfo:
        f1.result()
    assert "unlucky" in str(excinfo.value)
    pool.join()
    assert pool.terminated


def test_exception_lambda():
    pool = ProcessPool(ExceptionLambdaWorker, 1)
    f1 = pool.submit_job()
    with pytest.raises(Exception) as excinfo:
        f1.result()
    assert "Exception originally of type ExceptionWithLambda" in str(excinfo)
    assert "annoying" in str(excinfo)
    pool.join()
    assert pool.terminated


def test_exception_traceback():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 13)
    exc = f1.exception()
    assert exc is not None
    exc_traceback = "".join(traceback.TracebackException.from_exception(exc).format())
    assert "Traceback (most recent call last)" in exc_traceback
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


def test_run_job_exception():
    pool = ProcessPool(CrashingSquareNumberWorker, 1)
    assert 16 == pool.run_job("transform", 4)
    with pytest.raises(ValueError):
        pool.run_job("transform", 13)
    pool.join()
    assert pool.terminated


def test_pickle_bad_arg_returned():
    class BadObjectReturner:
        def run(self, *args):
            bad_list = []
            for _ in range(10000):
                bad_list = [0, bad_list]
            return bad_list

    pool = ProcessPool(BadObjectReturner, 1)
    f1 = pool.submit_job(":(")
    with pytest.raises(Exception) as excinfo:  # not working
        f1.result()
    assert "maximum recursion depth exceeded while pickling" in str(excinfo.value)
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated


def test_pickle_bad_arg_given():
    bad_arg = []
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
