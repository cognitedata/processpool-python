import time

from cognite.processpool import ProcessPool


class SquareNumberWorker:
    def run(self, msg, num):
        time.sleep(abs(0.02 * (num - 5)))
        return num * num


class PoolWorker:
    def run(self, num_workers):
        pool = ProcessPool(SquareNumberWorker, num_workers)
        jobs = [pool.submit_job("transform", i) for i in range(num_workers)]
        results = sum([j.result() for j in jobs])
        pool.join()
        return results


def test_nested():
    num_workers = 10
    pool = ProcessPool(PoolWorker, 10)
    jobs = [pool.submit_job(i) for i in range(num_workers)]
    results = [j.result() for j in jobs]
    assert [0, 0, 1, 5, 14, 30, 55, 91, 140, 204] == results
    assert not pool.terminated
    assert not pool.shutting_down
    pool.join()
    assert pool.terminated
