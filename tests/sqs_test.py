# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
import unittest
import uuid

#
# Third party libraries
#

from mock import MagicMock, patch
import simplejson

#
# Internal libraries
#

from krux_boto.boto import Boto3
from krux_sqs.sqs import Sqs


class SqsTest(unittest.TestCase):
    TEST_REGION = 'us-west-2'
    TEST_QUEUE_NAME = 'test-queue'

    TEST_RECEIPT_HANDLE = 't3st+R3c31pt/H4nDle'
    TEST_MESSAGE_ID = str(uuid.uuid4())
    TEST_BODY = {'foo': 'bar'}
    TEST_MESSAGE = MagicMock(
        receipt_handle=TEST_RECEIPT_HANDLE,
        message_id=TEST_MESSAGE_ID,
        body=simplejson.dumps(TEST_BODY),
        message_attributes=None,
        queue_url='https://queue.amazonaws.com/12345/' + TEST_QUEUE_NAME,
        attributes=None,
    )

    def setUp(self):
        self._logger = MagicMock()
        self._stats = MagicMock()

        self._resource = MagicMock(
            spec=Boto3().resource('sqs')
        )
        self._boto = MagicMock(
            spec=Boto3,
            resource=MagicMock(return_value=self._resource),
        )

        self._sqs = Sqs(
            boto=self._boto,
            logger=self._logger,
            stats=self._stats,
        )

    def test_init(self):
        """
        Sqs.__init__() correctly initialize internal fields
        """
        self.assertEqual(self._resource, self._sqs._resource)
        self._boto.resource.assert_called_once_with('sqs')
        self.assertEqual({}, self._sqs._queues)

    def test_init_not_implemented(self):
        """
        Sqs.__init__() correctly raise an error for non-supported Boto objects
        """
        with self.assertRaises(NotImplementedError):
            Sqs(
                boto=MagicMock()
            )

    def test_get_queue_no_cache(self):
        """
        Sqs._get_queue() correctly creates and caches the queue upon the first call
        """
        self._sqs._queues = {}
        expected = self._resource.get_queue_by_name.return_value

        self.assertEqual(expected, self._sqs._get_queue(SqsTest.TEST_QUEUE_NAME))

        self.assertEqual(expected, self._sqs._queues.get(SqsTest.TEST_QUEUE_NAME))
        self._resource.get_queue_by_name.assert_called_once_with(QueueName=SqsTest.TEST_QUEUE_NAME)

    def test_get_queue_cache(self):
        """
        Sqs._get_queue() correctly uses the cached queue object
        """
        expected = self._resource.get_queue_by_name.return_value
        self._sqs._queues = {
            SqsTest.TEST_QUEUE_NAME: expected
        }

        self.assertEqual(expected, self._sqs._get_queue(SqsTest.TEST_QUEUE_NAME))

        self.assertFalse(self._resource.get_queue_by_name.called)

    def test_get_messages_json(self):
        """
        Sqs.get_messages() correctly receives messages and converts them into dictionary
        """
        receive_messages = self._resource.get_queue_by_name.return_value.receive_messages
        receive_messages.return_value = [SqsTest.TEST_MESSAGE]
        expected = [{
            'ReceiptHandle': SqsTest.TEST_MESSAGE.receipt_handle,
            'MessageId': SqsTest.TEST_MESSAGE.message_id,
            'Body': SqsTest.TEST_BODY,
            'MessageAttributes': SqsTest.TEST_MESSAGE.message_attributes,
            'QueueUrl': SqsTest.TEST_MESSAGE.queue_url,
            'Attributes': SqsTest.TEST_MESSAGE.attributes,
        }]

        self.assertEqual(expected, self._sqs.get_messages(queue_name=SqsTest.TEST_QUEUE_NAME, is_json=True))

        self._resource.get_queue_by_name.assert_called_once_with(QueueName=SqsTest.TEST_QUEUE_NAME)
        receive_messages.assert_called_once_with(
            MessageAttributeNames=Sqs.DEFAULT_MESSAGE_ATTRIBUTE_NAME,
            MaxNumberOfMessages=Sqs.MAX_RECEIVE_MESSAGES_NUM,
            WaitTimeSeconds=Sqs.RECEIVE_MESSAGES_TIMEOUT,
        )
        self._logger.debug.assert_called_once_with(
            'Recieved %s messages from %s queue', 1, SqsTest.TEST_QUEUE_NAME
        )

    def test_get_messages_params(self):
        """
        Sqs.get_messages() correctly sends the parameters to Boto
        """
        attributes = []
        num_msg = 2
        timeout = 4

        self._sqs.get_messages(
            queue_name=SqsTest.TEST_QUEUE_NAME,
            message_attribute_names=attributes,
            num_msg=num_msg,
            timeout=timeout,
            is_json=True,
        )

        self._resource.get_queue_by_name.return_value.receive_messages.assert_called_once_with(
            MessageAttributeNames=attributes,
            MaxNumberOfMessages=num_msg,
            WaitTimeSeconds=timeout,
        )

    def test_get_messages_no_json(self):
        """
        Sqs.get_messages() correctly leaves the body as is when `is_json` is set to False
        """
        self._resource.get_queue_by_name.return_value.receive_messages.return_value = [SqsTest.TEST_MESSAGE]
        expected = [{
            'ReceiptHandle': SqsTest.TEST_MESSAGE.receipt_handle,
            'MessageId': SqsTest.TEST_MESSAGE.message_id,
            'Body': SqsTest.TEST_MESSAGE.body,
            'MessageAttributes': SqsTest.TEST_MESSAGE.message_attributes,
            'QueueUrl': SqsTest.TEST_MESSAGE.queue_url,
            'Attributes': SqsTest.TEST_MESSAGE.attributes,
        }]

        self.assertEqual(expected, self._sqs.get_messages(queue_name=SqsTest.TEST_QUEUE_NAME, is_json=False))

    def test_delete_messages(self):
        """
        Sqs.delete_messages() correctly deletes given messages
        """
        messages = [{
            'MessageId': SqsTest.TEST_MESSAGE_ID,
            'ReceiptHandle': SqsTest.TEST_RECEIPT_HANDLE,
        }, {
            'MessageId': str(SqsTest.TEST_MESSAGE_ID) + '1',
            'ReceiptHandle': SqsTest.TEST_RECEIPT_HANDLE + '1',
        }]

        self._sqs.delete_messages(SqsTest.TEST_QUEUE_NAME, messages)

        entries = [{
            'Id': SqsTest.TEST_MESSAGE_ID,
            'ReceiptHandle': SqsTest.TEST_RECEIPT_HANDLE,
        }, {
            'Id': SqsTest.TEST_MESSAGE_ID + '1',
            'ReceiptHandle': SqsTest.TEST_RECEIPT_HANDLE + '1',
        }]
        self._resource.get_queue_by_name.return_value.delete_messages.assert_called_once_with(
            Entries=entries
        )

        self._logger.debug.assert_called_once_with('Removing following messages: %s', entries)

    def test_delete_messages_empty(self):
        """
        Sqs.delete_messages() correctly does nothing for no messages
        """
        self._sqs.delete_messages(SqsTest.TEST_QUEUE_NAME, [])

        self._logger.debug.assert_called_once_with('Messages list is empty. Not deleting any messages.')
        self.assertFalse(self._resource.get_queue_by_name.return_value.delete_messages.called)

    @unittest.skip("Test queue missing. Skip for now.")
    def test_send_message(self):
        """
        SQS messages can be sent correctly
        """
        # TODO: This test needs to be improved using mock and stuff. But for the interest of time,
        # let's leave it at this minimal state.
        messages = [
            {'foo': 'bar'}, 'baz', 'num3', 'num4', 'num5', 'num6', 'num7', 'num8', 'num9', 'num10',
            'num11', 'num12'
        ]
        self._sqs.send_messages(self.TEST_QUEUE_NAME, messages)
