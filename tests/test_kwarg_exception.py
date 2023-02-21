import pytest

from cognite.processpool import ProcessPool
from cognite.processpool.processpool import JobFailedException


class KWArgException(Exception):
    def __init__(self, reason: str, response: str):
        super().__init__(reason, response)
        self.reason = reason
        self.response = response


class KWArgExceptionWorker:
    def run(self) -> None:
        raise KWArgException(reason="reason", response="response")


def test_exception_multiarg() -> None:
    pool = ProcessPool(KWArgExceptionWorker, 1)
    f1 = pool.submit_job()
    with pytest.raises(JobFailedException) as excinfo:
        f1.result()
    assert "response" in str(excinfo.value)
    pool.join()
    assert pool.terminated
