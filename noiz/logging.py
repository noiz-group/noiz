import logging

class MyFormatter(logging.Formatter):
    width = 40
    datefmt='%Y-%m-%d %H:%M:%S'

    def format(self, record):
        cpath = f'{record.module}:{record.funcName}:{record.lineno}'
        cpath = cpath[-self.width:].ljust(self.width)
        record.message = record.getMessage()
        s = f"{record.levelname:<7} | {self.formatTime(record, self.datefmt)} | {cpath} | {record.getMessage()}"

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


logger_config = dict({
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s | %(levelname)s | %(module)s | %(message)s',
        },
       'myFormatter':{
          '()': 'noiz.logging.MyFormatter',
       },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'stream': 'ext://sys.stdout',
            'formatter': 'myFormatter',
        },
        'file': {
            'class': 'logging.FileHandler',
            'level': 'DEBUG',
            'filename': 'noiz.log',
            'formatter': 'myFormatter',
        },
        'wsgi': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://flask.logging.wsgi_errors_stream',
            'formatter': 'myFormatter'
        },
    },
    'loggers': {
        'app': {
            'level': 'DEBUG',
            'handlers': [],
            'propagate': True,
        },
        'processing': {
            'level': 'DEBUG',
            'handlers': ['file', 'console'],
            'propagate': False,
            },
        'cli': {
            'level': 'DEBUG',
            'handlers': [],
            'propagate': True,
        },
    },
    'root': {
            'level': 'DEBUG',
            'handlers': ['file', 'console', ],
        }
}
)
