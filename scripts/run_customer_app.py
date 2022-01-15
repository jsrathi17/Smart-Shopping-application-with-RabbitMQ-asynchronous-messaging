import sys
# Update to path where your code is
sys.path.append('../')

from db_and_event_definitions import customers_database
from customer_app import CustomerEventConsumer
import argparse

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Send pick up or purchase shopping event message")
	parser.add_argument('--customer_id', '-c', type=str.lower,
						choices=customers_database.keys(), 
						help="Customer ID")
	args = parser.parse_args()

	customer_app = CustomerEventConsumer(customer_id=args.customer_id)
	customer_app.initialize_rabbitmq()
	print(" [*] Customer App with ID = {} waiting for CustomerEvents. \
		To exit press CTRL+C".format(customer_app.customer_id))
	customer_app.start_consuming()