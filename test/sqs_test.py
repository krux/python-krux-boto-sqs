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
# Internal libraries
#

import krux_boto.boto
import krux_sqs.sqs


class SqsTest(unittest.TestCase):
    TEST_REGION = 'us-west-2'
    TEST_QUEUE_NAME = 'jib-test'

    def setUp(self):
        self._sqs = krux_sqs.sqs.Sqs(
            boto=krux_boto.boto.Boto3(
                region=self.TEST_REGION
            )
        )

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

    def test_delete_messages(self):
        """
        SQS messages can be deleted correctly
        """
        # TODO: This test needs to be improved using mock and stuff. But for the interest of time,
        # let's leave it at this minimal state.
        messages = self._sqs.get_messages(self.TEST_QUEUE_NAME)
        self._sqs.delete_messages(self.TEST_QUEUE_NAME, messages)

    def test_send_message(self):
        """
        SQS messages can be sent correctly
        """
        # TODO: This test needs to be improved using mock and stuff. But for the interest of time,
        # let's leave it at this minimal state.
        messages = [{'foo': 'bar'}, 'baz']
        self._sqs.send_messages(self.TEST_QUEUE_NAME, messages)
