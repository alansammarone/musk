import logging
from datetime import datetime

from musk.config import Config, DequeuerConfig
from musk.core import Processor, Queue


class Dequeuer:
    def __init__(
        self, queue: Queue, processor: Processor, config: Config = DequeuerConfig
    ):
        self._queue = queue
        self._processor = processor
        self._config = config
        self._logger = logging.getLogger(__name__)

    def _send_message_to_processor(self, message):

        self._logger.info(f"Processing message ({message.id}) : {message.body}")
        start = datetime.now()
        try:
            self._processor.process(message)
            message.delete()
        except BaseException:
            message.requeue()

        end = datetime.now()
        took = (end - start).total_seconds()
        took = round(took, 3)
        self._logger.info(f"Success processing message ({message.id}), took {took}s")

    def dequeue(self):

        self._logger.info(f"Reading queue {self._queue.get_queue_name()}...")
        number_of_messages_per_read = self._config.MESSAGES_PER_READ
        messages = self._queue.read(number_of_messages_per_read)
        for message in messages:
            self._send_message_to_processor(message)
