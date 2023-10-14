import warnings
from typing import Literal


class CatchWarningAsError:
    def __init__(
            self,
            warning_filter_action: Literal["error", "ignore", "always", "default", "module", "once"],
            warning_filter_message: str = ""
    ):
        self.warning_filter_action = warning_filter_action
        self.warning_filter_message = warning_filter_message

    def __enter__(self):
        warnings.filterwarnings(action=self.warning_filter_action, message=self.warning_filter_message)
        return self

    def __exit__(self, ex_type, ex_value, ex_traceback):
        warnings.resetwarnings()
        return
