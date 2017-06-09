#!flask/bin/python
from flask import Flask
from flask import request
from flask import Response
from flask import jsonify 
import uuid
import pika
import sys
import threading
import time

c = threading.Condition()
flag = 0      #shared between Thread_A and Thread_B
val = 20



app = Flask(__name__)
status_dict = {}
bs = threading.Lock()
#status_channel = {}


#globvar = 0
status_dict = {}

def set_dict_value(str, value):
    global status_dict    # Needed to modify global copy of globvar
    status_dict[str] = value
    #globvar = 1

def getDictValue(str):
	global status_dict
	if uuid in status_dict:
		return status_dict[str]
	else:
		return 2 #
    #print(globvar)     # No need for global declaration to read value of globvar

def getLengthOfDictionary():
	global status_dict
	return  str(len(status_dict))
	
def printEntireDictionary():
	global status_dict
	print status_dict



@app.route('/',methods=['POST'])
def index():
	#global status_dict
	print ""
	print "in index!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
	print "Length of dictionary: " + getLengthOfDictionary()
	#global task_queue_connection
	
	global bs
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
	
	bs.acquire()
	set_dict_value(str(theID), 1)
	#status_dict[str(theID)] = 1
	bs.release() 
	
	resp = Response()
	resp.headers['Location'] = '/' + str(theID)
	print "Length of dictionary: " + str(len(status_dict) )
	return resp,201



@app.route('/<uuid>/status')
def status(uuid):
	global bs
	bs.acquire()
	print ""
	print "In status!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! - looking for" + uuid
	progress = getDictValue(uuid)
	print "progress from status: " + str(progress)
	#global status_dict
	print "Length of dictionary: " + getLengthOfDictionary()

	#progress = status_dict[uuid]
	
	bs.release()
	
	
	if(progress == 0):
		print "progress: " + str(progress)
		res = { 'status' : 'QUEUED' , 'progress' : 0 }
	elif(progress == 100):
		res = { 'status' : 'DONE' , 'progress' : 100 }
	else:
		res = { 'status' : 'PROCESSING' , 'progress' : progress }
	print "Length of dictionary: " + getLengthOfDictionary()
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

	
	
	
	#global status_dict
	print ""
	print "in callback!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
	print "Length of dictionary: " + getLengthOfDictionary()
	
	ch.basic_ack(delivery_tag = method.delivery_tag) #should probably be moved to end of callback()
	
	global bs
	#print "body:" + body
	words = body.split()
	uuid = words[0]
	percentage = words[1]
	processing_node = words[2]
	#if uuid in status_dict:
	#	print "1: " + str(status_dict[uuid])
	#if uuid in status_dict:
	#	print status_dict[uuid]
	print "Callback: getDictValue: " + str(getDictValue(uuid))
	print "Callback: printEntireDictionary: "
	printEntireDictionary()
	print ""
	
	
	
	bs.acquire()
	#status_dict[uuid] = int(percentage)
	set_dict_value(uuid, int(percentage))
	#if uuid in status_dict:
	#print "Strange thing has happended. Must have been an old file that has been converted..."
	
	
	
	bs.release() 
	#if uuid in status_dict:
	print "Callback 2: " + str(getDictValue(uuid))
		#str(status_dict[uuid])
	#if uuid in status_dict:
	#	print status_dict[uuid]
	#print "percentage: " + str(int(percentage))
	#print "uuid: " + uuid
	#print "The file with the UUID '" + uuid + "' is finished to " + percentage + "%."
	#ch.basic_ack(delivery_tag = method.delivery_tag)
	print "Length of dictionary: " + getLengthOfDictionary()#str(len(status_dict) )
	print "Callback: printEntireDictionary: "
	printEntireDictionary()
	print ""

def start_consum():
	#global status_channel
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
	status_channel.start_consuming()


if __name__ == '__main__':
	global status_channel
	#initializing task_queue
	task_queue_connection = pika.BlockingConnection(pika.ConnectionParameters(
		host='localhost'))
	task_queue_channel = task_queue_connection.channel()
	task_queue_channel.queue_declare(queue='task_queue', durable=True)

	
	t = threading.Thread(target = start_consum , args = [])
	t.start()
	
	app.run(debug=True)
	
	
	
	
	
#~ class Thread_A(threading.Thread):
    #~ def __init__(self, name):
        #~ threading.Thread.__init__(self)
        #~ self.name = name

    #~ def run(self):
        #~ global flag
        #~ global val     #made global here
        #~ start_consum()
        
        
        #~ while True:
            #~ c.acquire()
            #~ if flag == 0:
                #~ print "A: val=" + str(val)
                #~ time.sleep(0.1)
                #~ flag = 1
                #~ val = 30
                #~ c.notify_all()
            #~ else:
                #~ c.wait()
            #~ c.release()


#~ class Thread_B(threading.Thread):
    #~ def __init__(self, name):
        #~ threading.Thread.__init__(self)
        #~ self.name = name

    #~ def run(self):
        #~ global flag
        #~ global val    #made global here
        #~ app.run(debug=True)
        #~ while True:
            #~ c.acquire()
            #~ if flag == 1:
                #~ print "B: val=" + str(val)
                #~ time.sleep(0.5)
                #~ flag = 0
                #~ val = 20
                #~ c.notify_all()
            #~ else:
                #~ c.wait()
            #~ c.release()


#~ a = Thread_A("myThread_name_A")
#~ b = Thread_B("myThread_name_B")

#~ b.start()
#~ a.start()

#~ a.join()
#~ b.join()	





