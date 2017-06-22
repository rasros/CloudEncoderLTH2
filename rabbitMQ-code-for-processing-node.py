#!/usr/bin/env python
import pika
import time

#this code is to be used in processing node

#initializing task queue

# Step #1: Connect to RabbitMQ
#parameters = pika.URLParameters('amqp://guest:guest@localhost:5672/%2F')

#task_queue_connection = pika.SelectConnection(parameters=parameters,
#                                   on_open_callback=on_open)
                                   
                                   
task_queue_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
task_queue_channel = task_queue_connection.channel()
task_queue_channel.queue_declare(queue='task_queue', durable=True)


#initializing status queue
status_connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
status_channel = status_connection.channel()
status_channel.queue_declare(queue='status_queue', durable=True)

print(' [*] Waiting for files to convert. To exit press CTRL+C')

#callback is processed when task is received
def callback(ch, method, properties, body):
    print(" [x] Received file to convert: %r" % body)
    fileNameToBeConverted = body
    #get file from Swift
    #start converting
    time.sleep(1)
    print "Converted 10%. Sending status update to Entry."
    status_channel.basic_publish(exchange='',
                      routing_key='status_queue',
                      body=fileNameToBeConverted + " 10",
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
    time.sleep(3)
    print "Converted 50%. Sending status update to Entry."
    status_channel.basic_publish(exchange='',
                      routing_key='status_queue',
                      body=fileNameToBeConverted + " 50",
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
    time.sleep(3)
    #send file to Swift
    print "Converted 100%. Sending status update to Entry."
    status_channel.basic_publish(exchange='',
                      routing_key='status_queue',
                      body=fileNameToBeConverted + " 100",
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
    print(" [x] Done. Sending Ack that task is completed")
    ch.basic_ack(delivery_tag = method.delivery_tag) 

task_queue_channel.basic_qos(prefetch_count=1)
task_queue_channel.basic_consume(callback,
                      queue='task_queue')

task_queue_channel.start_consuming()
