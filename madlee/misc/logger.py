import logging
from logging.config import dictConfig


def create_logger(name):
    result = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'normal': {
                'format': '{asctime} [{module}-{lineno}] {message}',
                'style': '{',
                'datefmt' : '%H:%M:%S'
            },
            'simple': {
                'format': '{message}',
                'style': '{',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
            },
            name:  {
                'level': 'INFO',
                'class': 'logging.handlers.TimedRotatingFileHandler',
                'filename': 'logs/%s.log' % name,
                'when': 'midnight',
                'backupCount': 7,
                'encoding': 'utf-8',
                'formatter': 'normal'
            }
        },
        'loggers': {
            'django':  {
                'handlers': ['console', name],
                'level': 'INFO'
            }
        }
    }
    dictConfig(result)
    return logging.getLogger(name)


