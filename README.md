
<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite ProcessPool Library
===========================

[![Release Status](https://github.com/cognitedata/processpool-python/workflows/release/badge.svg)](https://github.com/cognitedata/processpool-python/actions)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Library for multiprocessing which guarantees worker processes restarting on death.

Example use
-----------

Basic use:    
    
    from cognite.processpool import ProcessPool, WorkerDiedException    
    class SquareNumberWorker:
        def run(self, num, *args):
            return num * num

    pool = ProcessPool(SquareNumberWorker, 4)
    futures = [pool.submit_job(i) for i in range(100)]
    result = [f.result() for f in futures]
    pool.join()


With a process that throws an exception:

    from cognite.processpool import ProcessPool
    class BadWorker:
        def run(self, num, *args):
            raise ValueError()
    
    
    pool = ProcessPool(BadWorker, 1)
    future = pool.submit_job(1)
    try:
        future.result()  # raises ValueError
    except Exception as e:
        print("Raised", e.__class__.__name__)
    pool.join()

With a process that dies:

    from cognite.processpool import ProcessPool, WorkerDiedException
    class CrashingWorker:
        def run(self, num, *args):
            a = []
            while True:
                a.extend([0] * 1000000)
    
    
    pool = ProcessPool(CrashingWorker, 1)
    future = pool.submit_job(1)
    try:
        future.result()  # raises WorkerDiedException and restarts the worker
    except WorkerDiedException as e:
        print("Worker died with code", e.code)
    pool.join()


