import multiprocessing
import random
import string


class ResultProcess(multiprocessing.Process):
    def __init__(self, target, **kwargs) -> None:
        self._target = target
        self._queue = multiprocessing.Queue()
        self._failed = False
        kwargs.setdefault("daemon", True)
        super().__init__(target=self._run, **kwargs)

    def _run(self, *args, **kwargs) -> None:
        try:
            self._queue.put(self._target(*args, **kwargs))
        except Exception as ex:
            self._failed = True
            self._queue.put(ex)

    def get_result(self, timeout: int = None):
        try:
            result = self._queue.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("Operation timed out.")
        if self._failed:
            raise result
        return result


def generate_token(length: int) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))
