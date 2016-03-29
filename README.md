# krux_sqs

`krux_sqs` is a library that provides wrapper functions for common SQS usage. It uses `krux_boto` to connect to AWS SQS.

## Warning

In the current version, `krux_sqs.sqs.Sqs` is only compatible with `krux_boto.boto.Boto3` object. Passing other objects, such as `krux_boto.boto.Boto`, will cause an exception.

## Application quick start

The most common use case is to build a CLI script using `krux_boto.cli.Application`.
Here's how to do that:

```python

from krux_boto.cli import Application
from krux_sqs.sqs import Sqs

def main():
    # The name must be unique to the organization. The object
    # returned inherits from krux.cli.Application, so it provides
    # all that functionality as well.
    app = Application(name='krux-my-boto-script')

    sqs = Sqs(boto=app.boto3)
    for msg in sqs.get_messages(queue_name='my-test-queue'):
        print msg['MessageId'], msg['ReceiptHandle']

### Run the application stand alone
if __name__ == '__main__':
    main()

```

As long as you get an instance of `krux_boto.boto.Boto3`, the rest are the same. Refer to `krux_boto` module's [README](../krux_boto/README.md) on various ways to instanciate the class.
