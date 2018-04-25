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
import string

#
# Third party libraries
#

from mock import MagicMock, patch, call
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

    TEST_GROUP_ID = 5

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

    def test_get_random_id(self):
        """
        Sqs._get_random_id() correctly generate a random string
        """
        # TODO: Need a way to determine whether this is really a *random* string
        random_chars = string.ascii_lowercase + string.digits

        str = self._sqs._get_random_id()

        # Verify all characters are alphanumeric
        self.assertTrue(all(char in random_chars for char in str))

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

    def test_send_messages(self):
        """
        Sqs.send_messages() correctly sends given messages
        """
        dict_msg = {'foo': 'bar'}
        str_msg = 'baz'
        messages = [dict_msg, str_msg]
        sqs_msgs = [
            {'Id': SqsTest.TEST_MESSAGE_ID, 'MessageBody': simplejson.dumps(dict_msg)},
            {'Id': SqsTest.TEST_MESSAGE_ID, 'MessageBody': str_msg},
        ]

        with patch('krux_sqs.sqs.Sqs._get_random_id', return_value=SqsTest.TEST_MESSAGE_ID):
            self._sqs.send_messages(SqsTest.TEST_QUEUE_NAME, messages)

        self._resource.get_queue_by_name.return_value.send_messages.assert_called_once_with(
            Entries=sqs_msgs
        )
        self._logger.debug.assert_called_once_with('Sending following messages: %s', sqs_msgs)

    def test_send_messages_empty(self):
        """
        Sqs.send_messages() correctly does nothing for no messages
        """
        self._sqs.send_messages(SqsTest.TEST_QUEUE_NAME, [])

        self._logger.debug.assert_called_once_with('Message is empty. Not sending any messages.')
        self.assertFalse(self._resource.get_queue_by_name.return_value.send_messages.called)

    def test_send_messages_invalid_type(self):
        """
        Sqs.send_messages() correctly errors upon invalid message type
        """
        with self.assertRaises(TypeError) as e:
            self._sqs.send_messages(SqsTest.TEST_QUEUE_NAME, [1])

        self.assertEqual('Message must be either a dictionary or a string', str(e.exception))

    def test_send_messages_chunk(self):
        """
        Sqs.send_messages() correctly divides up messages in chunks
        """
        iteration = 2
        messages = [str(i) for i in range(0, Sqs.MAX_SEND_MESSAGES_NUM * iteration)]

        with patch('krux_sqs.sqs.Sqs._get_random_id', return_value=SqsTest.TEST_MESSAGE_ID):
            self._sqs.send_messages(SqsTest.TEST_QUEUE_NAME, messages)

        send_calls = []
        for i in range(0, iteration):
            sqs_msgs = [
                {'Id': SqsTest.TEST_MESSAGE_ID, 'MessageBody': msg}
                for msg in messages[Sqs.MAX_SEND_MESSAGES_NUM * i:Sqs.MAX_SEND_MESSAGES_NUM * (i + 1)]
            ]
            send_calls.append(call(Entries=sqs_msgs))

        self.assertEqual(send_calls, self._resource.get_queue_by_name.return_value.send_messages.call_args_list)

    def test_send_messages_group_id(self):
        """
        Sqs.send_messages() correctly sends given group ID if provided
        """
        messages = ['foo']
        sqs_msgs = [
            {'Id': SqsTest.TEST_MESSAGE_ID, 'MessageBody': msg, 'MessageGroupId': SqsTest.TEST_GROUP_ID}
            for msg in messages
        ]

        with patch('krux_sqs.sqs.Sqs._get_random_id', return_value=SqsTest.TEST_MESSAGE_ID):
            self._sqs.send_messages(SqsTest.TEST_QUEUE_NAME, messages, SqsTest.TEST_GROUP_ID)

        self._resource.get_queue_by_name.return_value.send_messages.assert_called_once_with(
            Entries=sqs_msgs
        )
