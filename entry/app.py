#!flask/bin/python
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify
import uuid
import pika
import sys
import threading
from control.openstack import OpenStackVMOperations

lock = threading.Lock()

app = Flask(__name__)
status_dict = {}
status_channel = {}

#initializing status queue
conPara = pika.ConnectionParameters('waspmq',5672,'/',
        credentials=pika.PlainCredentials("test", "test")
        )
status_connection = pika.BlockingConnection(conPara)
status_channel = status_connection.channel()
status_channel.queue_declare(queue='status_queue', durable=True)

@app.route('/',methods=['POST'])
def index():
	global status_dict
	global status_channel
	videoFile = request.files['file'];
	theID = uuid.uuid4()

	UUIDToBeConverted = str(theID)
        os = OpenStackVMOperations()
        os.readConf()
        swift = os.swiftConn()
        swift.put_container(UUIDToBeConverted)
        swift.put_object(UUIDToBeConverted, 'in.mp4',
            contents= videoFile.read(),
            content_type='video/mp4')
	task_queue_channel.basic_publish(exchange='',
                routing_key='task_queue',
                body=UUIDToBeConverted,
                properties=pika.BasicProperties(
                delivery_mode = 2,
                # make message persistent
                ))
	print("  [x] Sent request for converting %r" % UUIDToBeConverted)

	status_dict[str(theID)] = 1
	resp = Response()
	resp.headers['Location'] = '/' + str(theID)
	return resp,201

@app.route('/<uuid>/status')
def status(uuid):

	global status_dict
	global status_channel#global status_dict
	print "  STATUS: Length of dictionary: " + str(len(status_dict) )

	if(status_dict.has_key(str(uuid))):
		progress = status_dict[str(uuid)]
	else:
		print "  STATUS: dictionary has not: " + uuid
		progress = 0

	if(progress == 0):
		print "  STATUS: progress: " + str(progress)
		res = { 'status' : 'QUEUED' , 'progress' : 0 }
	elif(progress == 100):
		res = { 'status' : 'DONE' , 'progress' : 100 }
	else:
		res = { 'status' : 'PROCESSING' , 'progress' : progress }
	print "  Status respons for " + str(uuid) + " is: " + str(res)
	return jsonify(res)


@app.route('/<uuid>/download')
def download(uuid):
	resp = Response()
	resp.headers['Content-disposition'] = 'attachment; filename=' + uuid + '.mp4'
	theFile = open(uuid + '.mp4','rb')
	resp.data = theFile.read()
	index = 0
	return resp

def callback(ch, method, properties, body):

	global status_dict
	global status_channel

	print "  Callback: Received body:" + body
	words = body.split()
	uuid = words[0]
	percentage = words[1]
	status_dict[str(uuid)] = int(percentage)
	ch.basic_ack(delivery_tag = method.delivery_tag) # should be first or last in callback function??????????????????????????

def start_consum():
	global status_channel
	status_channel.start_consuming()

status_channel.basic_qos(prefetch_count=1) #only sends one message at the time and waits for the ack
status_channel.basic_consume(callback,
                      queue='status_queue')

if __name__ == '__main__':
	task_queue_connection = pika.BlockingConnection(conPara)
	task_queue_channel = task_queue_connection.channel()
	task_queue_channel.queue_declare(queue='task_queue', durable=True)

	print('  [*] Waiting for status messages. To exit press CTRL+C')

	t = threading.Thread(target = start_consum , args = [])
	t.start()

	app.run(debug=False,host="0.0.0.0") #DEBUG SHOULD ALWAYS BE FALSE!!!!!!!!!!!!!!!!!!!!!!!!!!!


