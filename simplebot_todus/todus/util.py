import random
import signal
import string


class timeout:
    def __init__(self, seconds=1, message="Timeout") -> None:
        self.seconds = seconds
        self.message = message

    def handle_timeout(self, signum, frame) -> None:
        raise TimeoutError(self.message)

    def __enter__(self) -> None:
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback) -> None:
        signal.alarm(0)


def generate_token(length: int) -> str:
    chars = string.ascii_letters + string.digits
    return "".join(random.choice(chars) for _ in range(length))
