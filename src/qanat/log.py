import logging

from qanat.context import QanatContextFilter

logger = logging.getLogger("qanat")
logger.addFilter(QanatContextFilter())

# Prevent "No handlers found" warning if user doesn't configure logging
logger.addHandler(logging.NullHandler())


def instrument_logging(default_value: str = "-"):
    old_factory = logging.getLogRecordFactory()

    qanat_filter = QanatContextFilter(default_value)

    def qanat_record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        qanat_filter.filter(record)
        return record

    logging.setLogRecordFactory(qanat_record_factory)
