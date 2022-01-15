import sys
# Update to path where your code is
sys.path.append('../')
import argparse
from worker import ShoppingWorker

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Run RabbitExerciseShoppingWorker")
	parser.add_argument('--id', '-i', type=str, help="Worker ID", required=True)
	parser.add_argument('--queue', '-q', type=str, help="Queue bound to consistent hash exchange", required=True)
	parser.add_argument('--weight', '-w', type=str, 
					    help="Binding weight for queue bound to consistent hash exchange", required=False)
	args = parser.parse_args()

	worker = ShoppingWorker(args.id, args.queue, args.weight)
	
	worker.initialize_rabbitmq()
	print(' [*] Worker waiting for ShoppingEvents. To exit press CTRL+C')
	worker.start_consuming()