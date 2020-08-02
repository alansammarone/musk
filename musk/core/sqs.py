import json
import logging
import time
import uuid

import boto3

logger = logging.getLogger(__file__)


class Queue:
    pass


class Message:
    pass


class SQSMessage:
    """
        This class is a simple wrapper around
        boto's message class.

    """

    # This is the SQS resource, needed when instatiating new messages
    # not coming directly from a queue resource
    _sqs = None

    @classmethod
    def get_new_sqs_resource(self):
        """
            Returns a new sqs object instance,
            using credentials present in ~/.aws
            or in env vars.
        """
        sqs = boto3.resource("sqs")
        return sqs

    @property
    def id(self):
        return self._message.message_id

    @property
    def body(self):
        # We only use json messages
        return json.loads(self._message.body)

    @property
    def attributes(self):
        return self._messsage.attributes

    def delete(self):
        return self._message.delete()

    def requeue(self):
        self._message.change_visibility(VisibilityTimeout=0)

    def __init__(self, message):
        # This represents the internal boto3 message object
        # which this class is a wrapper for
        self._message = message

    def serialize(self):
        return dict(
            init_args=dict(
                queue_url=self._message.queue_url,
                receipt_handle=self._message.receipt_handle,
            ),
            message_attributes=self._message.message_attributes,
        )


class SQSQueue(Queue):

    # These are all SQS queue properties
    sqs_message_retention_period: int = 60 * 60 * 24 * 4  # 4 Days
    sqs_visibility_timeout: int = 60

    # These are internal properties, used only by this code
    _sleep_interval: int
    _queue_environment: str

    # Internal representation of SQS objects
    _sqs: int
    _queue: int

    # This is a constant that provides the map
    # SQS Property name -> Internal property name
    QUEUE_PROPERTIES_MAPPING = {
        "VisibilityTimeout": "sqs_visibility_timeout",
        "MessageRetentionPeriod": "sqs_message_retention_period",
    }
    # This is the error code present in the factory exceptions.
    # We need to check this error code string to know which exception occured.
    NONEXISTANT_QUEUE_ERROR_CODE = "AWS.SimpleQueueService.NonExistentQueue"

    def __init__(
        self, queue_environment: str, sleep_interval: int = 2,
    ):

        self.queue_environment = queue_environment
        self._sleep_interval = sleep_interval

        self._sqs = None
        self._queue = None

    def get_sqs_resource(self):

        if not self._sqs:
            self._sqs = self.get_new_sqs_resource()
        return self._sqs

    def get_new_sqs_resource(self):
        """
            Returns a new sqs object instance,
            using credentials present in ~/.aws
            or in env vars.
        """
        sqs = boto3.resource("sqs")
        return sqs

    def get_queue_resource(self):

        if not self._queue:
            self._queue = self.get_new_queue_resource()

        return self._queue

    def is_queue_doesnt_exist_exception(self, exception):
        """
            This checks whether a given exception
            represents a NONEXISTANT_QUEUE error.
            We need this because boto3 doesn't yet have
            exception classes for all errors
        """
        try:
            error_code = exception.response["Error"]["Code"]
            is_queue_doesnt_exist_exception = (
                error_code == self.NONEXISTANT_QUEUE_ERROR_CODE
            )
        except:
            logger.exception("Exception checking if queue exists:")
            is_queue_doesnt_exist_exception = False

        return is_queue_doesnt_exist_exception

    def get_queue_environment(self):

        """
            Returns a string representing the environment we're in,
            such as PROD, DEV or LOCAL
        """
        if not self._queue_environment:
            self._queue_environment = self.get_queue_environment_from_config()
        return self._queue_environment

    def get_new_queue_resource(self):
        """
            Returns a (new) reference to the queue object
        """
        sqs = self.get_sqs_resource()
        try:
            queue = sqs.get_queue_by_name(QueueName=self.get_queue_name())
        except Exception as exception:
            if self.is_queue_doesnt_exist_exception(exception):
                queue = self.create_queue()
            else:
                raise
        return queue

    def get_queue_attributes(self):
        queue_attributes = {}
        for sqs_property, internal_property in self.QUEUE_PROPERTIES_MAPPING.items():
            queue_attributes[sqs_property] = getattr(self, internal_property)
            # Apparently all parameter values must be strings
            queue_attributes[sqs_property] = str(queue_attributes[sqs_property])

        return queue_attributes

    def create_queue(self):

        sqs = self.get_sqs_resource()
        queue_name = self.get_queue_name()
        logger.info(f"Creating queue {queue_name}")
        queue = sqs.create_queue(
            QueueName=queue_name, Attributes=self.get_queue_attributes()
        )
        return queue

    def write(self, messages):
        """
            Writes multiple messages to the queue
        """
        queue = self.get_queue_resource()
        if type(messages) is not list:
            raise TypeError("Argument to write should be a list.")

        messages = [
            {"MessageBody": json.dumps(message), "Id": str(uuid.uuid4())}
            for message in messages
        ]
        response = queue.send_messages(Entries=messages)
        self._assert_write_succesfull(response)

    def _assert_write_succesfull(self, response):
        if response["ResponseMetadata"]["HTTPStatusCode"] != 200:
            logger.exception(
                f"Message write unsuccessful: {json.dumps(response, indent=4)}"
            )

            raise
        if "Failed" in response:
            logger.exception(
                f"Not all messages sent succesfully:  {json.dumps(response, indent=4)}"
            )

    def read(self, max_number_of_messages: int = 1):
        """
            Reads the messages present in the queue *right now*
            and returns as soon as the queue is empty
        """
        queue = self.get_queue_resource()
        for message in queue.receive_messages(
            MaxNumberOfMessages=max_number_of_messages
        ):

            yield SQSMessage(message)

    def read_forever(self, max_number_of_messages_per_read: int = 1):
        """
            Keeps reading and yielding messages forever,
            with a small interval between successive reads
        """
        while True:
            for message in self.read(max_number_of_messages_per_read):
                yield message
            logger.info("Sleeping...")
            time.sleep(self._sleep_interval)

    def get_queue_name(self):
        name = f"{self.name}_{self.queue_environment}"
        return name


# class OCPublicCompanyQueue(SQSQueue):
#     name = "oc_publiccompany_pg_{queue_env}"


# A = OCPublicCompanyQueue()
# b = A.write([{"my_cool_atr": 1}, {"my_cool_attr": 2}])
# A.write([{"my_cool_atr": 1}])
# A.write([{"my_cool_atr": 1}])


# for message in A.read_forever():
#     print(message)
#     print(type(message.body))
#     message.delete()


# class MessageService:
#     def read_from_queue(self):
#         pass
