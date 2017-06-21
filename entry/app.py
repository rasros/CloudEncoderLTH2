#!/usr/bin/env python2

#!flask/bin/python
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
import uuid
import pika
import sys
import threading
from control.openstack import WaspSwiftConn
from control.keyval import KeyValueStore
from control.notify import NotifyThread
import os
import traceback

lock = threading.Lock()

app = Flask(__name__)
status_dict = {}
status_channel = {}
keyval = None
myname = "entry"

def log(s):
	global keyval,myname
	print(s)
	keyval.log(myname, s)

#initializing status queue
conPara = pika.ConnectionParameters('waspmq',5672,'/',
        credentials=pika.PlainCredentials("test", "test")
        )
status_connection = pika.BlockingConnection(conPara)
status_channel = status_connection.channel()
status_channel.queue_declare(queue='status_queue', durable=True)

@app.route('/',methods=['POST'])
def index():
	try:
		global status_dict
		global status_channel
		global task_queue_channel
		videoFile = request.files['file'];
		theID = uuid.uuid4()

		log("Index request")

		UUIDToBeConverted = str(theID)
		ostack = WaspSwiftConn()
		ostack.readConf()
		swift = ostack.swiftConn()
		swift.put_container(UUIDToBeConverted)
		swift.put_object(UUIDToBeConverted, 'in.mp4',
            contents= videoFile.read(),
            content_type='video/mp4')
		swift.close()
		task_queue_channel.basic_publish(exchange='',
                routing_key='task_queue',
                body=UUIDToBeConverted,
                properties=pika.BasicProperties(
                delivery_mode = 2,
                # make message persistent
                ))
		log("  [x] Sent request for converting %r" % UUIDToBeConverted)

		status_dict[str(theID)] = 0
		resp = Response()
		resp.headers['Location'] = '/' + str(theID)
		return resp,201
	except Exception as e:
		exc_type, exc_value, exc_traceback = sys.exc_info()
		log("Exception {}".format(e))
		for l in traceback.format_exception(exc_type, exc_value, exc_traceback):
			log("E " + l)
	return False

@app.route('/<uuid>/status')
def status(uuid):

	global status_dict
	global status_channel#global status_dict
	log("  STATUS: Length of dictionary: " + str(len(status_dict) ))

	if(status_dict.has_key(str(uuid))):
		progress = status_dict[str(uuid)]
	else:
		log("  STATUS: dictionary has not: " + uuid)
		progress = 0

	if(progress == 0):
		log("  STATUS: progress: " + str(progress))
		res = { 'status' : 'QUEUED' , 'progress' : 0 }
	elif(progress == 100):
		res = { 'status' : 'DONE' , 'progress' : 100 }
        elif(progress < 0):
                res = { 'status' : 'FAILED', 'progress' : -1}
	else:
		res = { 'status' : 'PROCESSING' , 'progress' : progress }
	log("  Status respons for " + str(uuid) + " is: " + str(res))
	return jsonify(res)


@app.route('/<uuid>/download')
def download(uuid):
	try:
		log("Download request")
		resp = Response()
		resp.headers['Content-disposition'] = 'attachment; filename=' + uuid + '.mp4'
		ostack = WaspSwiftConn()
		ostack.readConf()
		swift = ostack.swiftConn()
		obj = swift.get_object(uuid, 'out.mp4')
		resp.data = obj[1]
		swift.close()
		return resp
	except Exception as e:
		log(e)
	return False

def callback(ch, method, properties, body):
	try:
		global status_dict
		global status_channel

		log("  Callback: Received body:" + body)
		words = body.split()
		uuid = words[0]
		percentage = words[1]
		status_dict[str(uuid)] = int(percentage)
		ch.basic_ack(delivery_tag = method.delivery_tag) # should be first or last in callback function?
	except Exception as e:
		log(e)
	return False

def start_consum():
	global status_channel
	status_channel.start_consuming()

status_channel.basic_qos(prefetch_count=1) #only sends one message at the time and waits for the ack
status_channel.basic_consume(callback,
                      queue='status_queue')

def main():
	global keyval,app,task_queue_channel,myname
	keyval = KeyValueStore(host=sys.argv[1])

	myname = "entry:"+os.uname()[1]
	task_queue_connection = pika.BlockingConnection(conPara)
	task_queue_channel = task_queue_connection.channel()
	task_queue_channel.queue_declare(queue='task_queue', durable=True)

	log('  [*] Waiting for status messages. To exit press CTRL+C')

	notifyThread = NotifyThread(host=sys.argv[1])
	notifyThread.start()

	t = threading.Thread(target = start_consum , args = [])
	t.start()

	app.run(debug=False,host="0.0.0.0") #DEBUG SHOULD ALWAYS BE FALSE!

if __name__ == '__main__':
	try:
		main()
	except Exception as e:
		log(e)

