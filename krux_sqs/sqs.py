# -*- coding: utf-8 -*-
#
# © 2016-2019 Salesforce.com, inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
import uuid

#
# Third party libraries
#

import simplejson
import six

#
# Internal libraries
#

from krux.logging import get_logger
from krux.stats import get_stats
from krux.cli import get_parser
from krux_boto.boto import Boto3, add_boto_cli_arguments


NAME = 'krux-sqs'


def get_sqs(args=None, logger=None, stats=None):
    """
    Return a usable Sqs object without creating a class around it.

    In the context of a krux.cli (or similar) interface the 'args', 'logger'
    and 'stats' objects should already be present. If you don't have them,
    however, we'll attempt to provide usable ones for the SQS setup.

    (If you omit the add_sqs_cli_arguments() call during other cli setup,
    the Boto object will still work, but its cli options won't show up in
    --help output)

    (This also handles instantiating a Boto3 object on its own.)
    """
    if not args:
        parser = get_parser()
        add_sqs_cli_arguments(parser)
        args = parser.parse_args()

    if not logger:
        logger = get_logger(name=NAME)

    if not stats:
        stats = get_stats(prefix=NAME)

    boto = Boto3(
        log_level=args.boto_log_level,
        access_key=args.boto_access_key,
        secret_key=args.boto_secret_key,
        region=args.boto_region,
        logger=logger,
        stats=stats,
    )
    return Sqs(
        boto=boto,
        logger=logger,
        stats=stats,
    )


def add_sqs_cli_arguments(parser, include_boto_arguments=True):
    """
    Utility function for adding SQS specific CLI arguments.
    """
    if include_boto_arguments:
        # GOTCHA: Since many modules use krux_boto, the krux_boto's CLI arguments can be included twice,
        # causing an error. This creates a way to circumvent that.

        # Add all the boto arguments
        add_boto_cli_arguments(parser)


class Sqs(object):
    """
    A manager to handle all SQS related functions.
    Each instance is locked to a connection to a designated region (self.boto.cli_region).
    """

    # This is the maximum allowed by Boto3
    MAX_RECEIVE_MESSAGES_NUM = 10
    MAX_SEND_MESSAGES_NUM = 10

    # Arbitrarily chosen
    # According to AWS docs, the valid values are integers between 1 and 20:
    # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
    RECEIVE_MESSAGES_TIMEOUT = 10
    # Always receives all message attributes
    DEFAULT_MESSAGE_ATTRIBUTE_NAME = ['All']

    def __init__(
        self,
        boto,
        logger=None,
        stats=None,
    ):
        """
        Basic init
        :param boto: Boto object to be used as an API library to talk to AWS
        :type boto: krux_boto.boto.Boto3
        :param logger: Logger, recommended to be obtained using krux.cli.Application
        :type logger: logging.Logger
        :param stats: Stats, recommended to be obtained using krux.cli.Application
        :type stats: kruxstatsd.StatsClient
        """
        # Private variables, not to be used outside this module
        self._name = NAME
        self._logger = logger or get_logger(self._name)
        self._stats = stats or get_stats(prefix=self._name)

        if not isinstance(boto, Boto3):
            raise NotImplementedError('Currently krux_boto.sqs.Sqs only supports krux_boto.boto.Boto3')

        self._resource = boto.resource('sqs')
        self._queues = {}

    @staticmethod
    def _get_random_id():
        return str(uuid.uuid4())[:8]

    def _get_queue(self, queue_name):
        """
        Returns a queue with the given name.
        The queue is fetched on the first call (lazy) and cached.

        :param queue_name: Name of the queue to get.
        :type queue_name: str
        """
        if self._queues.get(queue_name, None) is None:
            self._queues[queue_name] = self._resource.get_queue_by_name(QueueName=queue_name)

        return self._queues[queue_name]

    def get_messages(
        self,
        queue_name,
        message_attribute_names=DEFAULT_MESSAGE_ATTRIBUTE_NAME,
        num_msg=MAX_RECEIVE_MESSAGES_NUM,
        timeout=RECEIVE_MESSAGES_TIMEOUT,
        is_json=True,
    ):
        """
        Returns a list of messages in the given queue.

        Note that not all messages may be returned:
        http://boto3.readthedocs.org/en/latest/reference/services/sqs.html#SQS.Queue.receive_messages

        :param queue_name: Name of the queue to get messages from.
        :type queue_name: str
        :param message_attribute_names: The names of the message attributes to receive.
                                        Refer to Boto3 doc for more info.
        :type message_attribute_names: list
        :param num_msg: Maximum number of messages to get.
        :type num_msg: int
        :param timeout: Timeout (in seconds) limit for receiving messages.
        :type timeout: int
        :param is_json: If True, assumes the body of the message is stringified JSON and tries to parse it.
                        Leaves as string otherwise.
        :type is_json: bool
        :return: List of messages from the given SQS queue
        :rtype: list[dict[str, Any]]
        """
        raw_messages = self._get_queue(queue_name).receive_messages(
            MessageAttributeNames=message_attribute_names,
            MaxNumberOfMessages=num_msg,
            WaitTimeSeconds=timeout
        )
        self._logger.debug('Recieved %s messages from %s queue', len(raw_messages), queue_name)

        result = []
        for msg in raw_messages:
            # Parse the strings as JSON so that we can deal with them easier
            if is_json:
                body = simplejson.loads(msg.body)
            else:
                body = msg.body

            msg_dict = {
                'ReceiptHandle': msg.receipt_handle,
                'MessageId': msg.message_id,
                'Body': body,
                'MessageAttributes': msg.message_attributes,
                'QueueUrl': msg.queue_url,
                'Attributes': msg.attributes,
            }
            result.append(msg_dict)

        return result

    def delete_messages(self, queue_name, messages):
        """
        Deletes the given list of messages from the given queue.

        :param queue_name: Name of the queue to delete messages from.
        :type queue_name: str
        :param messages: List of messages returned by get_messages().
        :type messages: list
        :rtype: None
        """
        # GOTCHA: queue.delete_messages() does not handle an empty list
        if len(messages) > 0:
            entries = [{'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']} for msg in messages]

            self._logger.debug('Removing following messages: %s', entries)
            self._get_queue(queue_name).delete_messages(
                Entries=entries
            )
        else:
            self._logger.debug('Messages list is empty. Not deleting any messages.')

    def send_messages(self, queue_name, messages, group_id=None):
        """
        Send the given list of messages to the given queue.

        :param queue_name: Name of the queue to send messages.
        :type queue_name: str
        :param messages: List of message to send. If a message is dict, it will be stringified as JSON object.
        :type messages: list | str
        :param group_id: Message group id if send to FIFO queue.
        :type group_id: int
        :rtype: None
        """
        # GOTCHA: queue.send_message() does not handle an empty message
        if messages:
            entries = []

            for message in messages:
                if isinstance(message, dict):
                    msg = simplejson.dumps(message)
                elif isinstance(message, str):
                    msg = message
                else:
                    raise TypeError('Message must be either a dictionary or a string')

                entry = {
                    'Id': Sqs._get_random_id(),
                    'MessageBody': msg,
                }
                if group_id is not None:
                    entry['MessageGroupId'] = group_id
                entries.append(entry)

            self._logger.debug('Sending following messages: %s', entries)
            q = self._get_queue(queue_name)
            for i in six.moves.range(0, len(entries), self.MAX_SEND_MESSAGES_NUM):
                chunk = entries[i:i + self.MAX_SEND_MESSAGES_NUM]
                q.send_messages(Entries=chunk)
        else:
            self._logger.debug('Message is empty. Not sending any messages.')
