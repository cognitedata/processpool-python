import pickle


class RandomObject:
    def __init__(self, reason: str, response: str):
        self.reason = reason
        self.response = response

    def __new__(cls, *args, **kwargs):
        print("new!")
        instance = cls(*args, **kwargs)
        instance.args = args

    def __reduce__(self):
        return (self.__class__, self.args, self.__dict__)


obj = RandomObject(reason="reason", response="response")
pickled = pickle.dumps(obj)
print(pickle.loads(pickled))
