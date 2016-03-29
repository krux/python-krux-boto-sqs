# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
from abc import ABCMeta, abstractmethod

#
# Third party libraries
#

import simplejson

#
# Internal libraries
#

from krux.logging import get_logger
from krux.stats import get_stats
from krux.cli import get_parser, get_group
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

    # Add those specific to the application
    group = get_group(parser, NAME)


class Sqs(object):
    """
    A manager to handle all SQS related functions.
    Each instance is locked to a connection to a designated region (self.boto.cli_region).
    """

    # This is the maximum allowed by Boto3
    MAX_RECEIVE_MESSAGES_NUM = 10
    # Arbitrarily chosen
    # According to AWS docs, the valid values are integers between 1 and 20:
    # http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
    MAX_RECEIVE_MESSAGES_WAIT = 10

    def __init__(
        self,
        boto,
        logger=None,
        stats=None,
    ):
        # Private variables, not to be used outside this module
        self._name = NAME
        self._logger = logger or get_logger(self._name)
        self._stats = stats or get_stats(prefix=self._name)

        if not isinstance(boto, Boto3):
            raise NotImplementedError('Currently krux_boto.sqs.Sqs only supports krux_boto.boto.Boto3')

        self._resource = boto.resource('sqs')
        self._queues = {}

    def _get_queue(self, queue_name):
        """
        Returns a queue with the given name.
        The queue is fetched on the first call (lazy) and cached.

        :param queue_name: :py:class:`str` Name of the queue to get.
        """
        if self._queues.get(queue_name, None) is None:
            self._queues[queue_name] = self._resource.get_queue_by_name(QueueName=queue_name)

        return self._queues[queue_name]

    def get_messages(self, queue_name):
        """
        Returns a list of messages in the given queue.

        Note that not all messages may be returned:
        http://boto3.readthedocs.org/en/latest/reference/services/sqs.html#SQS.Queue.receive_messages

        Expects the body and body.Message to be stringified JSON values, and thus tries to parse it.
        May throw simplejson.JSONDecodeError if unable to parse the values.

        :param queue_name: :py:class:`str` Name of the queue to get messages from.
        """
        raw_messages = self._get_queue(queue_name).receive_messages(
            MaxNumberOfMessages=self.MAX_RECEIVE_MESSAGES_NUM,
            WaitTimeSeconds=self.MAX_RECEIVE_MESSAGES_WAIT
        )

        result = []
        for msg in raw_messages:
            # Parse the strings as JSON so that we can deal with them easier
            body_dict = simplejson.loads(msg.body)
            body_dict['Message'] = simplejson.loads(body_dict['Message'])

            msg_dict = {
                'ReceiptHandle': msg.receipt_handle,
                'MessageId': msg.message_id,
                'Body': body_dict,
                'MessageAttributes': msg.message_attributes,
                'QueueUrl': msg.queue_url,
                'Attributes': msg.attributes,
            }
            result.append(msg_dict)

        return result

    def delete_messages(self, queue_name, messages):
        """
        Deletes the given list of messages from the given queue.

        :param queue_name: :py:class:`str` Name of the queue to delete messages from.
        :param messages: :py:class:`list` List of messages returned by get_messages().
        """
        # GOTCHA: queue.delete_messages() does not handle an empty list
        if len(messages) > 0:
            self._get_queue(queue_name).delete_messages(
                Entries=[{'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']} for msg in messages]
            )
