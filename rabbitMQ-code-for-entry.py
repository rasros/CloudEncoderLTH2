#!/usr/bin/env python
import pika
import sys

#this code is to be used in Entry node

#initializing task_queue
task_queue_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
task_queue_channel = task_queue_connection.channel()
task_queue_channel.queue_declare(queue='task_queue', durable=True)

#initializing status queue
status_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
status_channel = status_connection.channel()
status_channel.queue_declare(queue='status_queue', durable=True)


#Sending 10 new tasks to task queue
for index in range(0,10):
    #fileNameToBeConverted = ' '.join(sys.argv[1:]) or "f8kj38jfl31dl0"
    fileNameToBeConverted = str(index) + ".mp4"
    print "Uploading file '" + fileNameToBeConverted + "' to Swift."
    task_queue_channel.basic_publish(exchange='',
                      routing_key='task_queue',
                      body=fileNameToBeConverted,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
    print(" [x] Sent request for converting %r" % fileNameToBeConverted)
task_queue_connection.close()


#Receiving status updates. If many Entry nodes is to be used, pub/sub should be used
# instead of working queues.
print(' [*] Waiting for status messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    words = body.split()
    uuid = words[0]
    percentage = words[1]
    print "The file with the UUID '" + uuid + "' is finished to " + percentage + "%."
    ch.basic_ack(delivery_tag = method.delivery_tag)

status_channel.basic_qos(prefetch_count=1) #only sends one message at the time and waits for the ack
status_channel.basic_consume(callback,
                      queue='status_queue')

status_channel.start_consuming()




#to see all queues and number of messages in queues:
#sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged

