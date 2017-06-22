import pika
import time


pika_conn_params = pika.ConnectionParameters(
    host='localhost', port=5672,
    credentials=pika.credentials.PlainCredentials('guest', 'guest'),
)
connection = pika.BlockingConnection(pika_conn_params)
channel = connection.channel()

while(True):
	
	queue = channel.queue_declare(
		queue="task_queue", durable=True,
		exclusive=False, auto_delete=False
	)
	print(queue.method.message_count)
	time.sleep(1)

