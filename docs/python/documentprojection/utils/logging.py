import logging

logging.basicConfig(
    level="INFO", format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
)


def config_log(level: str):
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
        force=True,
    )


def get_log(name: str):
    return logging.getLogger(name)
