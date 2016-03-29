# -*- coding: utf-8 -*-
#
# © 2016 Krux Digital, Inc.
#

#
# Standard libraries
#

from __future__ import absolute_import
import os

#
# Internal libraries
#

from krux.cli import get_group
import krux_boto.cli
from krux_sqs.sqs import add_sqs_cli_arguments, get_sqs, NAME, Sqs


class Application(krux_boto.cli.Application):

    def __init__(self, name=NAME):
        # Call to the superclass to bootstrap.
        super(Application, self).__init__(name=name)

        self.sqs = get_sqs(self.args, self.logger, self.stats)

    def add_cli_arguments(self, parser):
        # Call to the superclass
        super(Application, self).add_cli_arguments(parser)

        add_sqs_cli_arguments(parser, include_boto_arguments=False)

        group = get_group(parser, self.name)

        group.add_argument(
            'queue_name',
            type=str,
            help='Name of the SQS queue to get messages from',
        )
        group.add_argument(
            '-n', '--num-msg',
            type=int,
            default=Sqs.MAX_RECEIVE_MESSAGES_NUM,
            help="Maximum number of messages to get (default: %(default)s)",
        )
        group.add_argument(
            '-t', '--timeout',
            type=int,
            default=Sqs.RECEIVE_MESSAGES_TIMEOUT,
            help="Timeout (in seconds) limit for receiving messages (default: %(default)s)",
        )

    def run(self):
        print self.sqs.get_messages(
            queue_name=self.args.queue_name,
            num_msg=self.args.num_msg,
            timeout=self.args.timeout,
        )


def main():
    app = Application()
    with app.context():
        app.run()


# Run the application stand alone
if __name__ == '__main__':
    main()
