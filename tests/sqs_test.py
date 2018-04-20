# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
import unittest

#
# Third party libraries
#

from mock import MagicMock, patch
import boto3

#
# Internal libraries
#

from krux_boto.boto import Boto3
from krux_sqs.sqs import Sqs

class SqsTest(unittest.TestCase):
    TEST_REGION = 'us-west-2'
    TEST_QUEUE_NAME = 'seth-test-1'

    def setUp(self):
        self._logger = MagicMock()
        self._stats = MagicMock()

        self._resource = MagicMock()
        self._boto = MagicMock(
            spec=Boto3,
            resource=MagicMock(return_value=self._resource),
        )

        self._sqs = Sqs(
            boto=self._boto
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

    @unittest.skip("Test queue missing. Skip for now.")
    def test_get_messages(self):
        """
        SQS messages are received and converted into dictionary correctly
        """
        # TODO: This test needs to be improved using mock and stuff. But for the interest of time,
        # let's leave it at this minimal state.
        messages = self._sqs.get_messages(self.TEST_QUEUE_NAME)
        self.assertIsInstance(messages, list)

        for msg in messages:
            self.assertIn('ReceiptHandle', msg)
            self.assertIsInstance(msg['ReceiptHandle'], str)
            self.assertIn('MessageId', msg)
            self.assertIsInstance(msg['MessageId'], str)
            self.assertIn('Body', msg)
            self.assertIsInstance(msg['Body'], dict)
            self.assertIn('Message', msg['Body'])
            self.assertIsInstance(msg['Body']['Message'], dict)
            self.assertIn('MessageAttributes', msg)
            self.assertIn('QueueUrl', msg)
            self.assertIn('Attributes', msg)

    @unittest.skip("Test queue missing. Skip for now.")
    def test_delete_messages(self):
        """
        SQS messages can be deleted correctly
        """
        # TODO: This test needs to be improved using mock and stuff. But for the interest of time,
        # let's leave it at this minimal state.
        messages = self._sqs.get_messages(self.TEST_QUEUE_NAME)
        self._sqs.delete_messages(self.TEST_QUEUE_NAME, messages)

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
