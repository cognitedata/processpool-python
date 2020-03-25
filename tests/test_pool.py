import time

import pytest

from cognite.processpool import ProcessPool, WorkerDiedException


class SquareNumberWorker:
    def run(self, msg, num, *args):
        print("in run", msg, num, args)
        time.sleep(0.1)
        if num == 1337:
            a = []
            while True:
                a.extend([0] * 1000000)
        if num == 13:
            raise ValueError("Got unlucky!")
        return num * num


def test_simple():
    pool = ProcessPool(SquareNumberWorker, 1)
    f1 = pool.run_job("transform", 2)
    f2 = pool.run_job("transform", 3)
    assert 4 == f1.result()
    assert 9 == f2.result()
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated


def test_crash():
    pool = ProcessPool(SquareNumberWorker, 1)
    f1 = pool.run_job("transform", num=1)
    f2 = pool.run_job("transform", 1337)
    f3 = pool.run_job("transform", 3)
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()

    assert 1 == f1.result()
    assert 9 == f3.result()
    with pytest.raises(WorkerDiedException):
        f2.result()


def test_exception():
    pool = ProcessPool(SquareNumberWorker, 1)
    f1 = pool.run_job("transform", 13)
    with pytest.raises(ValueError) as excinfo:
        f1.result()
    assert "unlucky" in str(excinfo.value)
