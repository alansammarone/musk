import logging
from musk.core import Message


class Processor:
    def __init__(self):
        self._logger = logging.getLogger(__name__)

    def process(self, message: Message):
        raise NotImplemented
