import argparse
import time

from google.cloud import pubsub_v1

def receive_messages(project, subscription_name):

    """Receives messages from a pull subscription."""

   

    subscriber = pubsub_v1.SubscriberClient()

    subscription_path = subscriber.subscription_path(

        project, subscription_name)

 

    def callback(message):

        print('Received message: {}'.format(message))

        message.ack()

 

    subscriber.subscribe(subscription_path, callback=callback)

 

    # The subscriber is non-blocking, so we must keep the main thread from

    # exiting to allow it to process messages in the background.

    print('Listening for messages on {}'.format(subscription_path))

    while True:

        time.sleep(60)
		
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('project', help='Your Google Cloud project ID')
	
subparsers = parser.add_subparsers(dest='command')
receive_parser = subparsers.add_parser(
	'receive', help=receive_messages.__doc__)
receive_parser.add_argument('subscription_name')

args = parser.parse_args()
receive_messages(args.project, args.subscription_name)