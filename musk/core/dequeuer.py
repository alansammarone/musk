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

    def _log_message_start(self, message):
        message_id = message.id
        message_body = message.body.copy()
        for key, value in message_body.items():

            if isinstance(value, list) and len(value) > 16:
                value_repr = f"[{value[0]}, {value[1]}, {value[2]}, {value[3]}, ...] ({len(value)} elements)"
                message_body[key] = value_repr

        self._logger.info(f"Processing message ({message_id}): {message_body}")

    def _send_message_to_processor(self, message):

        self._log_message_start(message)

        start = datetime.now()
        try:
            self._processor.process(message)
            message.delete()
            end = datetime.now()
            took = (end - start).total_seconds()
            took = round(took, 3)
            self._logger.info(
                f"Success processing message ({message.id}), took {took}s"
            )
        except BaseException:
            self._logger.exception("Exception in processor: ")
            message.requeue()

    def dequeue(self):

        self._logger.info(f"Reading queue {self._queue.get_queue_name()}...")
        number_of_messages_per_read = self._config.MESSAGES_PER_READ
        messages = self._queue.read(number_of_messages_per_read)
        for message in messages:
            self._send_message_to_processor(message)
