#!flask/bin/python
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify 
import uuid
import pika
import sys


app = Flask(__name__)

@app.route('/',methods=['POST'])
def index():
	global task_queue_connection
	videoFile = request.files['file'];
	theID = uuid.uuid4()
	videoFile.save(str(theID) + ".mp4")
	
	UUIDToBeConverted = str(theID)
	task_queue_channel.basic_publish(exchange='',
					  routing_key='task_queue',
					  body=UUIDToBeConverted,
					  properties=pika.BasicProperties(
						 delivery_mode = 2, # make message persistent
					  ))
	print(" [x] Sent request for converting %r" % UUIDToBeConverted)
  
	
	resp = Response()
	resp.headers['Location'] = '/' + str(theID)
	return resp,201



@app.route('/<uuid>/status')
def status(uuid):
	global status_dict
	progress = status_dict[uuid]
	
	if(progress == 0):
		res = { 'status' : 'QUEUED' , 'progress' : 0 }
	elif(progress == 100):
		res = { 'status' : 'DONE' , 'progress' : 100 }
	else:
		res = { 'status' : 'PROCESSING' , 'progress' : progress }
	return jsonify(res)


@app.route('/<uuid>/download')
def download(uuid):
	resp = Response()
	resp.headers['Content-disposition'] = 'attachment; filename=' + uuid + '.mp4'
	theFile = open(uuid + '.mp4','rb')
	resp.data = theFile.read()
	index = 0
	return resp
	

status_dict = {}

def callback(ch, method, properties, body):
	global status_dict
	words = body.split()
	uuid = words[0]
	percentage = words[1]
	processing_node = words[2]
	status_dict[uuid] = percentage
	print "The file with the UUID '" + uuid + "' is finished to " + percentage + "%." + 
	ch.basic_ack(delivery_tag = method.delivery_tag)

def start_consum():
	global status_channel
	status_channel.start_consuming()


if __name__ == '__main__':
	
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
	
	#starting status queue
	print(' [*] Waiting for status messages. To exit press CTRL+C')
	status_channel.basic_qos(prefetch_count=1) #only sends one message at the time and waits for the ack
	status_channel.basic_consume(callback,
					  queue='status_queue')
	t = threading.Thread(target = start_consum , args = [])
	t.start()
	
	app.run(debug=True)
	
	





