from typing import Any, Tuple

import pytest

from cognite.processpool import ProcessPool, ProcessPoolShutDownException


class SquareNumberWorker:
    def run(self, msg: str, num: float, *args: Tuple[Any, ...]) -> float:
        return num * num


def test_simple() -> None:
    pool = ProcessPool(SquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 2)
    f2 = pool.submit_job("transform", 3)
    assert 4 == f1.result()
    assert 9 == f2.result()
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated


def test_submit_or_join_after_join() -> None:
    pool = ProcessPool(SquareNumberWorker, 1)
    f1 = pool.submit_job("transform", 0)
    assert 0 == f1.result()
    pool.join()
    assert pool.terminated
    with pytest.raises(ProcessPoolShutDownException):
        pool.submit_job("transform", 13)
    with pytest.raises(ProcessPoolShutDownException):
        pool.join()
    assert pool.terminated


def test_many() -> None:
    pool = ProcessPool(SquareNumberWorker, 4)
    futures = [pool.submit_job(i) for i in range(100)]
    result = [f.result() for f in futures]
    assert [n * n for n in range(100)] == result
    pool.join()
