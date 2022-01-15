from types import SimpleNamespace

import pika
import json
import dateutil.parser
import time
from db_and_event_definitions import customers_database, cost_per_unit, number_of_units, BillingEvent, ProductEvent
from xprint import xprint

class ShoppingWorker:

    def __init__(self, worker_id, queue, weight="1"):
        # Do not edit the init method.
        # Set the variables appropriately in the methods below.
        self.connection = None
        self.channel = None
        self.worker_id = worker_id
        self.queue = queue
        self.weight = weight
        self.shopping_state = {}
        self.shopping_events = []
        self.billing_event_producer = None
        self.customer_app_event_producer = None

    def initialize_rabbitmq(self):
        # To implement - Initialize the RabbitMQ connection, channel, exchange and queue here
        # Also initialize the channels for the billing_event_producer and customer_app_event_producer
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='shopping_events_exchange', exchange_type='x-consistent-hash', durable = True)
        self.channel.exchange_declare(exchange = 'shopping_dead_letter_exchange', exchange_type = 'direct', durable = True)

        self.channel.queue_declare(queue='shopping_events_dead_letter_queue')
        self.channel.queue_declare(queue=self.queue, arguments = {'x-dead-letter-exchange':'shopping_dead_letter_exchange','x-dead-letter-routing-key':'dead_key'})
        self.channel.queue_bind(exchange = 'shopping_events_exchange', queue=self.queue, routing_key = self.weight)
        self.channel.queue_bind(exchange = 'shopping_dead_letter_exchange', queue = 'shopping_events_dead_letter_queue', routing_key = 'dead_key')
        
        

        self.billing_event_producer = BillingEventProducer(self.connection,self.worker_id)
        self.billing_event_producer.initialize_rabbitmq()
        self.customer_app_event_producer =  CustomerEventProducer(self.connection,self.worker_id)
        self.customer_app_event_producer.initialize_rabbitmq()

        xprint("ShoppingWorker {}: initialize_rabbitmq() called".format(self.worker_id))

    def handle_shopping_event(self, ch, method, properties, body):
        # To implement - This is the callback that is passed to "on_message_callback" when a message is received
        shopping_event = json.loads(body)
        productevent = ProductEvent(shopping_event['event_type'], shopping_event['product_number'], shopping_event['timestamp'])
        customer = self.get_customer_id_from_shopping_event(productevent)
        if not customer:
            self.channel.basic_reject(delivery_tag = method.delivery_tag, requeue = False)
            return

        if shopping_event['event_type'] =='pick up':
            self.shopping_state[shopping_event['product_number']] = shopping_event['timestamp']
        elif shopping_event['event_type'] == 'purchase':
            shopping_cost = 0.80 * cost_per_unit * number_of_units
            timestamp = self.shopping_state.pop(shopping_event['product_number'])
            billing_event = BillingEvent(customer, shopping_event['product_number'], timestamp, shopping_event['timestamp'], shopping_cost)
            self.billing_event_producer.publish(billing_event)
            self.customer_app_event_producer.publish_billing_event(billing_event)
        self.shopping_events.append(productevent)
        self.customer_app_event_producer.publish_shopping_event(customer, productevent)
        xprint("ShoppingWorker {}: handle_event() called".format(self.worker_id))
        self.channel.basic_ack(delivery_tag = method.delivery_tag)
        # Handle the application logic and the publishing of events here

    # Utility function to get the customer_id from a shopping event
    def get_customer_id_from_shopping_event(self, shopping_event):
        customer_id = [customer_id for customer_id, product_number in customers_database.items()
                       if shopping_event.product_number == product_number]
        if len(customer_id) is 0:
            xprint("{}: Customer Id for product number {} Not found".format(self.worker_id, shopping_event.product_number))
            return None
        return customer_id[0]

    def start_consuming(self):
        # To implement - Start consuming from Rabbit
        self.channel.basic_consume(on_message_callback = self.handle_shopping_event, queue =self.queue, auto_ack = False)
        xprint("ShoppingWorker {}: start_consuming() called".format(self.worker_id))
        self.channel.start_consuming()

    def close(self):
        # Do not edit this method
        try:
            xprint("Closing worker with id = {}".format(self.worker_id))
            self.channel.stop_consuming()
            time.sleep(1)
            self.channel.close()
            self.billing_event_producer.close()
            self.customer_app_event_producer.close()
            time.sleep(1)
            self.connection.close()
        except Exception as e:
            print("Exception {} when closing worker with id = {}".format(e, self.worker_id))


class BillingEventProducer:

    def __init__(self, connection, worker_id):
        # Do not edit the init method.
        self.worker_id = worker_id
        # Reusing connection created in ShoppingWorker
        self.channel = connection.channel()

    def initialize_rabbitmq(self):
        self.channel.queue_declare(queue = 'billing_events', durable = True)
        xprint("ShoppingEventProducer initialize_rabbitmq() called")
        # To implement - Initialize the RabbitMq connection, channel, exchange and queue here
        xprint("BillingEventProducer {}: initialize_rabbitmq() called".format(self.worker_id))

    def publish(self, billing_event):
        
        self.channel.basic_publish(exchange='', routing_key = 'billing_events', body=json.dumps(vars(billing_event)))

        xprint("BillingEventProducer {}: Publishing billing event {}".format(
            self.worker_id,
            vars(billing_event)))
        # To implement - publish a message to the Rabbitmq here
        # Use json.dumps(vars(billing_event)) to convert the shopping_event object to JSON

    def close(self):
        # Do not edit this method
        self.channel.close()


class CustomerEventProducer:

    def __init__(self, connection, worker_id):
        # Do not edit the init method.
        self.worker_id = worker_id
        # Reusing connection created in ShoppingWorker
        self.channel = connection.channel()

    def initialize_rabbitmq(self):
        self.channel.exchange_declare(exchange='customer_app_events', exchange_type = 'topic')

        # To implement - Initialize the RabbitMq connection, channel, exchange and queue here
        xprint("CustomerEventProducer {}: initialize_rabbitmq() called".format(self.worker_id))

    def publish_billing_event(self, billing_event):
        self.channel.basic_publish(exchange='customer_app_events', routing_key = billing_event.customer_id, body=json.dumps(vars(billing_event)))
        xprint("{}: CustomerEventProducer: Publishing billing event {}"
              .format(self.worker_id, vars(billing_event)))
        # To implement - publish a message to the Rabbitmq here
        # Use json.dumps(vars(billing_event)) to convert the shopping_event object to JSON

    def publish_shopping_event(self, customer_id, shopping_event):
        self.channel.basic_publish(exchange='customer_app_events', routing_key = customer_id, body=json.dumps(vars(shopping_event)))
        xprint("{}: CustomerEventProducer: Publishing shopping event {} {}"
              .format(self.worker_id, customer_id, vars(shopping_event)))
        # To implement - publish a message to the Rabbitmq here
        # Use json.dumps(vars(shopping_event)) to convert the shopping_event object to JSON

    def close(self):
        # Do not edit this method
        self.channel.close()
