#rpc_server_processingnode1.py

# The file consumes a work package from the queue and converts the file. 
# Then returns an address of the converted file in respons to the client (Entry). 
# The problem with this solution is that the client will not receive any status updates,
# but will instead wait for the work to be completed, and will then receive the finished
# work.

#!/usr/bin/env python
import pika
import time

connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

channel = connection.channel()

channel.queue_declare(queue='rpc_queue')

def ConvertFile(SwiftInputAddress):
	print "processing file on address: '" + SwiftInputAddress + "'"
	return "outputAddress"
	
def on_request(ch, method, props, swiftAddress):
    newAddress = ConvertFile(swiftAddress)
    time.sleep(5)#Remove sleep!!!!!!!!!!!!!!!!!!!!!!!
    print "Converted file from address: '" + swiftAddress + "' to address: '" + newAddress + "'"
    ch.basic_publish(exchange='',
                     routing_key=props.reply_to,
                     properties=pika.BasicProperties(correlation_id = \
                                                         props.correlation_id),
                     body=newAddress)
    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(on_request, queue='rpc_queue')

print(" [x] Awaiting RPC requests")
channel.start_consuming()
