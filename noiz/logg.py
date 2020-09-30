import logging


class MyFormatter(logging.Formatter):
    width = 80
    datefmt = "%Y-%m-%d %H:%M:%S"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            from celery._state import get_current_task

            self.get_current_task = get_current_task
        except ImportError:
            self.get_current_task = lambda: None

    def format(self, record):
        task = self.get_current_task()
        if task and task.request:
            record.__dict__.update(task_id=task.request.id, task_name=task.name)
        else:
            record.__dict__.setdefault("task_name", "")
            record.__dict__.setdefault("task_id", "")

        lvl = f"{record.levelname:<7}"
        fmted_time = self.formatTime(record, self.datefmt)
        task_info = f"{record.task_id}|{record.task_name}"
        breadcrumbs = f"{record.module}:{record.funcName}:{record.lineno}"
        breadcrumbs = breadcrumbs[-self.width :].ljust(self.width)
        record.message = record.getMessage()

        s = f"{lvl} | {fmted_time} | {task_info} |{breadcrumbs} | {record.getMessage()}"

        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + record.exc_text
        if record.stack_info:
            if s[-1:] != "\n":
                s = s + "\n"
            s = s + self.formatStack(record.stack_info)

        return s


logger_config = dict(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
            },
            "myFormatter": {"()": "noiz.logg.MyFormatter"},
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "WARNING",
                "stream": "ext://sys.stdout",
                "formatter": "myFormatter",
            },
            "file": {
                "class": "logging.handlers.RotatingFileHandler",
                "level": "DEBUG",
                "filename": "noiz.log",
                "maxBytes":209715200,
                "backupCount": 20,
                "formatter": "myFormatter",
            },
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "myFormatter",
            },
        },
        "loggers": {
            "noiz.app": {
                "level": "DEBUG",
                "handlers": [],
                "propagate": True
            },
            "noiz.processing": {
                "level": "DEBUG",
                "handlers": [],
                "propagate": False,
            },
            "noiz.cli": {
                "level": "DEBUG",
                "handlers": [],
                "propagate": True
            },
            "noiz.api": {
                "level": "DEBUG",
                "handlers": [],
                "propagate": True
            },
        },
        "root": {"level": "DEBUG", "handlers": ["console", "file"]},
    }
)
