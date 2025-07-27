import logging
import logging.config
from pathlib import Path

LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {"format": "%(asctime)s [%(levelname)s] %(name)s - %(message)s"},
        "short": {"format": "[%(levelname)s] %(name)s: %(message)s"},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "short",
            "level": "DEBUG",
        },
        "pipeline_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "level": "DEBUG",
            "filename": str(LOGS_DIR / "pipeline.log"),
            "maxBytes": 5 * 1024 * 1024,
            "backupCount": 3,
            "encoding": "utf8",
        },
        "skipped_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "level": "INFO",
            "filename": str(LOGS_DIR / "skipped.log"),
            "maxBytes": 2 * 1024 * 1024,
            "backupCount": 2,
            "encoding": "utf8",
        },
        "validation_file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "level": "INFO",
            "filename": str(LOGS_DIR / "validation.log"),
            "maxBytes": 2 * 1024 * 1024,
            "backupCount": 2,
            "encoding": "utf8",
        },
        "derived_fields": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "default",
            "level": "INFO",
            "filename": str(LOGS_DIR / "derived_logs.log"),
            "maxBytes": 2 * 1024 * 1024,
            "backupCount": 2,
            "encoding": "utf8",
        },
    },
    "loggers": {"derived_fields": {"handlers": ["derived_fields"]}},
    "root": {"handlers": ["console", "pipeline_file"], "level": "DEBUG"},
}


def setup_logging():
    logging.config.dictConfig(LOG_CONFIG)
