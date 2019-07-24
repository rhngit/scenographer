import sys
import warnings
from io import StringIO
from typing import Callable


class PrintAs:
    def __init__(self, print_callable: Callable[[str], None], skip_empty=True):
        self.log_sink = StringIO()
        self.print_callable = print_callable
        if skip_empty:
            self._callable = self.print_skip_empty_callable
        else:
            self._callable = self.print_callable

    def __enter__(self):
        self.original_stdout = sys.stdout
        self.original_showwarning = warnings.showwarning
        sys.stdout = self.log_sink
        warnings.showwarning = lambda message, c, f, ln, fh, l: self._callable(message)

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.seek(0)
        self._callable(sys.stdout.read())
        sys.stdout.close()
        sys.stdout = self.original_stdout
        warnings.showwarning = self.original_showwarning

    def print_skip_empty_callable(self, message):
        message = str(message).strip()
        message = message.strip()
        if message:
            return self.print_callable(message)
