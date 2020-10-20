import time

import pytest

from cognite.processpool import JobTimedOutException, ProcessPool, ProcessPoolShutDownException


class SleepWorker:
    def run(self, msg, num, *args):
        time.sleep(num)
        return num * num


def test_cancel():
    pool = ProcessPool(SleepWorker, 2)
    fs = [pool.submit_job("transform", 0.5, timeout=1) for _ in range(10)]
    ff = pool.submit_job("transform", 1.5, timeout=1)
    fs += [pool.submit_job("transform", 0.5, timeout=1) for _ in range(10)]

    assert all(0.25 == f.result() for f in fs)
    with pytest.raises(JobTimedOutException) as excinfo:
        ff.result()

    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated
