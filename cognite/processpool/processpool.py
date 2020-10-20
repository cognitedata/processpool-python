import ctypes
import inspect
import multiprocessing
import pickle
import queue
import sys
import threading
import time
from concurrent.futures import Future
from typing import Any

from tblib import pickling_support

SLEEP_TICK = 0.001  # Duration in seconds used to sleep when waiting for results


class WorkerDiedException(Exception):
    """Raised when getting the result of a job where the process died while executing it for any reason."""

    def __init__(self, message, code=None):
        super().__init__(message, code)
        self.code = code
        self.message = message


class JobTimedOutException(Exception):
    """Raised when getting the result of a job where the time limit was exceeded."""


class JobFailedException(Exception):
    """Raised when a job fails with a normal exception."""

    def __init__(self, message, original_exception_type=None):
        super().__init__(message, original_exception_type)
        self.original_exception_type = original_exception_type
        self.message = message

    def __str__(self):
        return f"{self.__class__.__name__}: {self.original_exception_type}({self.message})"


class ProcessPoolShutDownException(Exception):
    """Raised when submitting jobs to a process pool that has been .join()ed or .terminate()d"""

    pass


def _async_raise(tid, exctype):
    """raises the exception, performs cleanup if needed"""
    if not inspect.isclass(exctype):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, 0)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class Thread(threading.Thread):  # http://tomerfiliba.com/recipes/Thread2/
    def _get_my_tid(self):
        """determines this (self's) thread id"""
        if not self.is_alive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for tid, tobj in threading._active.items():
            if tobj is self:
                self._thread_id = tid
                return tid

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exctype):
        """raises the given exception type in the context of this thread"""
        _async_raise(self._get_my_tid(), exctype)

    def terminate(self):
        """raises SystemExit in the context of the given thread, which should
        cause the thread to exit silently (unless caught)"""
        self.raise_exc(SystemExit)


@pickling_support.install
class _WrappedWorkerException(Exception):  # we need this since tracebacks aren't pickled by default and therefore lost
    def __init__(self, exception_str, exception_cls=None, traceback=None):
        # don't pickle problematic exception classes
        self.exception = JobFailedException(exception_str, exception_cls)
        if traceback is None:
            __, __, self.traceback = sys.exc_info()
        else:
            self.traceback = traceback


class _WorkerHandler:
    def __init__(self, worker_class: type):  # Runs in the main process
        self.worker_class = worker_class
        self.busy_with_future = None
        self.send_q = multiprocessing.Queue()  # type: multiprocessing.Queue
        self.recv_q = multiprocessing.Queue()  # type: multiprocessing.Queue
        self.process = multiprocessing.Process(
            target=self._worker_job_loop, args=(self.worker_class, self.recv_q, self.send_q)
        )
        self.process.start()

    def send(self, job):
        args, kwargs, future, timeout = job
        self.busy_with_future = future
        try:
            self.send_q.put(pickle.dumps((args, kwargs, timeout)))
        except Exception as error:  # pickle errors
            self.recv_q.put(pickle.dumps((None, _WrappedWorkerException(str(error), error.__class__.__name__))))

    def result(self):
        if not self.busy_with_future:
            return None
        try:
            ret, err = pickle.loads(self.recv_q.get(block=False))
            if err:
                unwrapped_err = err.exception  # unwrap
                unwrapped_err.__traceback__ = err.traceback
                err = unwrapped_err
            return ret, err
        except queue.Empty:
            if not self.process.is_alive():
                raise WorkerDiedException(
                    f"{self.process.name} terminated unexpectedly with exit code  {self.process.exitcode} while running job.",
                    self.process.exitcode,
                )
            return None

    @staticmethod
    def _worker_job_loop(
        worker_class: type, recv_q: multiprocessing.Queue, send_q: multiprocessing.Queue
    ):  # Runs in a subprocess
        worker = worker_class()
        BUSY = "__process_busy__"
        result = BUSY

        def run_job(args, kwargs):
            nonlocal result
            result = worker.run(*args, **kwargs)

        while True:
            args, kwargs, timeout = pickle.loads(send_q.get(block=True))
            try:
                error = None
                result = BUSY
                thread = Thread(target=run_job, args=(args, kwargs), daemon=True)
                thread.start()
                start_time = time.time()
                while result is BUSY:
                    if timeout is not None and time.time() - start_time > timeout:
                        thread.terminate()
                        result = None
                        error = JobTimedOutException(f"Job ran for more than {timeout} seconds and timed out")
                        break
                    time.sleep(SLEEP_TICK)
                thread.join()
            except MemoryError:  # py 3.8 consistent error
                raise WorkerDiedException(f"Process encountered MemoryError while running job.", "MemoryError")
            except Exception as e:
                error = _WrappedWorkerException(str(e), e.__class__.__name__)
                result = None
            try:
                recv_q.put(pickle.dumps((result, error)))
            except Exception as e:
                error = _WrappedWorkerException(str(e), e.__class__.__name__)
                recv_q.put(pickle.dumps((None, error)))


class ProcessPool:
    def __init__(self, worker_class: type, pool_size: int = 1, timeout=None):
        """Manages dispatching jobs to processes, checking results, sending them to futures and restarting if they die.

        Args:
            worker_class (Class): type that will receive the jobs in it's `run` method, one instance will be created per process, which should initialize itself fully.
            pool_size (int): number of worker processes to use.
        """
        self.worker_class = worker_class
        self.pool_size = pool_size
        self.shutting_down = False
        self.terminated = False
        self.timeout = timeout

        self._pool = [self._create_new_worker() for _ in range(pool_size)]

        self._job_queue = queue.Queue()  # type: queue.Queue # no need for a MP queue here
        self._job_loop = Thread(target=self._job_manager_thread, daemon=True)
        self._job_loop.start()

    def set_timeout(self, timeout):
        self.timeout = timeout

    def _create_new_worker(self):
        return _WorkerHandler(self.worker_class)

    def join(self):
        """Waits for jobs to finish and shuts down the pool."""
        self.shutting_down = True
        if self.terminated:
            raise ProcessPoolShutDownException("Can not join a WorkerPool that has been terminated")
        while not self._job_queue.empty() or any(worker.busy_with_future for worker in self._pool):
            time.sleep(SLEEP_TICK)
        self.terminate()  # could be gentler on the children

    def terminate(self):
        """Kills all sub-processes and stops the pool immediately."""
        self.terminated = True
        for worker in self._pool:
            worker.process.terminate()
        self._job_queue.put(None)  # in case it's blocking

    def _job_manager_thread(self):
        """Manages dispatching jobs to processes, checking results, sending them to futures and restarting if they die"""
        while True:
            busy_procs = []
            idle_procs = []
            for wix, worker in enumerate(self._pool):
                if worker.busy_with_future:
                    try:
                        result = worker.result()
                        if result is None:
                            busy_procs.append(wix)
                            continue
                        else:
                            result, exc = result
                    except WorkerDiedException as e:
                        if not self.terminated:
                            self._pool[wix] = self._create_new_worker()  # restart worker
                        result, exc = None, e
                    if exc:
                        worker.busy_with_future.set_exception(exc)
                    else:
                        worker.busy_with_future.set_result(result)  # Could be None
                    worker.busy_with_future = None  # done!
                else:
                    idle_procs.append(wix)

            if not idle_procs:
                time.sleep(SLEEP_TICK)
                continue

            if busy_procs:
                try:
                    job = self._job_queue.get(block=False)
                except queue.Empty:
                    time.sleep(SLEEP_TICK)
                    continue
            else:  # no jobs are running, so we can block
                job = self._job_queue.get(block=True)

            if job is None:
                return
            self._pool[idle_procs[0]].send(job)

    def submit_job(self, *args, **kwargs) -> Future:
        """Submits job asynchronously, which will eventually call the `run` method in worker_class with the arguments given, all of which should be picklable."""
        if self.terminated or self.shutting_down:
            raise ProcessPoolShutDownException("Worker pool shutting down or terminated, can not submit new jobs")
        future = Future()  # type: Future
        self._job_queue.put((args, kwargs, future, self.timeout))
        return future

    def run_job(self, *args, **kwargs) -> Any:
        """Submits job and blocks to wait for result. Returns the result or raises any Exception encountered. Should typically only be called from a thread."""
        return self.submit_job(*args, **kwargs).result()
