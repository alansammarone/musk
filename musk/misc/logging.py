import logging
import logging.config
import os

import yaml
from musk.config import LoggingConfig


def setup_logging(
    default_level: int = logging.INFO, default_format: str = "%(asctime)s %(message)s"
) -> None:
    config_filepath = LoggingConfig.CONFIG_FILEPATH
    if config_filepath and os.path.exists(config_filepath):
        loaded_from = config_filepath
        with open(config_filepath) as config_file:
            config = yaml.safe_load(config_file.read())
        logging.config.dictConfig(config)
    else:
        loaded_from = "default"
        logging.basicConfig(level=default_level, format=default_format)

    logger = logging.getLogger(__name__)
    logger.debug("Log config loaded from %s", loaded_from)
