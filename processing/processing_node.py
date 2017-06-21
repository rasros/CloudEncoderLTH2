#!/usr/bin/env python2
import pika
import time
import os
import errno
import uuid
from control.openstack import WaspSwiftConn
import transcode
import swiftclient
import traceback
from control.keyval import KeyValueStore
from control.notify import NotifyThread

class ProcessingNode:
    def __init__(self):
        #initializing task queue
        self.name = "work:"+os.uname()[1]
        self.kv = KeyValueStore(host='etcdhost')
        conPara = pika.ConnectionParameters('waspmq',5672,'/',
                credentials=pika.PlainCredentials("test", "test")
                )
        self.pika_connection = pika.BlockingConnection(conPara)
        self.task_channel = self.pika_connection.channel()
        self.task_channel.queue_declare(queue='task_queue', durable=True)
        self.task_channel.basic_qos(prefetch_count=1)
        self.task_channel.basic_consume(self.process, queue='task_queue')
        self.log(' [*] Waiting for files to convert. To exit press CTRL+C')
        self.task_channel.start_consuming()

    def log(self, s):
        print(s)
        self.kv.log(self.name, s)

    def progress(self, uuid, progress):
        self.status_channel.basic_publish(exchange='',
                          routing_key='status_queue',
                          body=uuid + " " + str(progress),
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))
        #self.pika_connection.process_data_events()

    # process is called when task is received
    def process(self, ch, method, properties, body):
        self.log(" [x] Received file to convert: %r" % body)
        uuid = body

        #initializing status queue
        self.status_channel = self.pika_connection.channel()
        self.status_channel.queue_declare(queue='status_queue', durable=True)

        self.progress(uuid, 1)

        conf = WaspSwiftConn()
        conf.readConf()
        swift = conf.swiftConn()

        #get file from Swift
        try:
            os.makedirs(uuid)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(uuid):
                pass
            else:
                raise

        try:
            obj_tuple = swift.get_object(uuid, 'in.mp4')
            with open(uuid + '/in.mp4', 'w') as file:
                file.write(obj_tuple[1])
        except swiftclient.exceptions.ClientException:
            self.log(" [x] Transcoding aborted, failed to fetch file for %r" % uuid)
            traceback.print_exc()
            self.progress(uuid, -1)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return
        
        self.log(" [x] Downloaded file.")

        transcode.do(uuid, self.progress)

        #send file to Swift
        try:
            with open(uuid + '/out.mp4', 'r') as file:
                swift.put_object(uuid, 'out.mp4', contents=file.read(), content_type='video/mp4')
        except swiftclient.exceptions.ClientException:
            self.log(" [x] Transcoding aborted, failed to upload file for %r" % uuid)
            traceback.print_exc()
            self.progress(uuid, -1)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return
 
        ch.basic_ack(delivery_tag = method.delivery_tag) 
        self.log(" [x] Transcoding done for %r." % uuid)

        self.log(" [x] Deleting input.")
        swift.delete_object(uuid, 'in.mp4')
        swift.close()
        self.progress(uuid, 100)

        self.status_channel.close()

def main():
	notifyThread = NotifyThread(host='etcdhost')
	notifyThread.start()
	node = ProcessingNode()

if __name__ == '__main__':
    node = ProcessingNode()
