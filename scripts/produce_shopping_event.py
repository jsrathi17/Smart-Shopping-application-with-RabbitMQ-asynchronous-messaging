import datetime
import sys
# Update to path where your code is
sys.path.append('../')

from db_and_event_definitions import ProductEvent
from shopping_sensor import ShoppingEventProducer
from worker import *
import argparse


def get_date_with_delta(minutes):
    d = datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes)
    return d.isoformat("T")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Send pick up or purchase shopping event message")
    parser.add_argument('--event', '-e', type=str.lower,
                        choices=['pick up', 'purchase'],
                        help="Type of event")
    parser.add_argument('--customer_id', '-c', type=str.lower,
                        choices=customers_database.keys(),
                        help="Customer ID")
    parser.add_argument('--timeoffset', '-t', type=int,
                        help='Number of minutes to add to current timestamp to check if the product is purchased',
                        default=0)
    args = parser.parse_args()

    shopping_event_producer = ShoppingEventProducer()
    shopping_event_producer.initialize_rabbitmq()
    shopping_event = ProductEvent(args.event,
                                 customers_database[args.customer_id],
                                 get_date_with_delta(args.timeoffset)
                                 )
    shopping_event_producer.publish(shopping_event)
    shopping_event_producer.close()
