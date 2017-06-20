#!/usr/bin/env python2
import pika
import time
import os
import uuid
from control.openstack import WaspSwiftConn
import transcode


class ProcessingNode:
    def __init__(self):
        #initializing task queue
        task_queue_connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        task_queue_channel = task_queue_connection.channel()
        task_queue_channel.queue_declare(queue='task_queue', durable=True)


        #initializing status queue
        status_connection = pika.BlockingConnection(pika.ConnectionParameters(
                host='localhost'))
        self.status_channel = status_connection.channel()
        self.status_channel.queue_declare(queue='status_queue', durable=True)

        task_queue_channel.basic_qos(prefetch_count=1)
        task_queue_channel.basic_consume(self.process,
                              queue='task_queue')

        task_queue_channel.start_consuming()
        print(' [*] Waiting for files to convert. To exit press CTRL+C')



    def progress(self, uuid, progress):
        self.status_channel.basic_publish(exchange='',
                          routing_key='status_queue',
                          body=uuid + " " + str(progress),
                          properties=pika.BasicProperties(
                             delivery_mode = 2, # make message persistent
                          ))

    # process is called when task is received
    def process(self, ch, method, properties, body):
        print(" [x] Received file to convert: %r" % body)
        uuid = body

        conf = WaspSwiftConn()
        conf.readConf()
        swift = conf.swiftConn()

        #get file from Swift
        os.makedirs(uuid)
        obj_tuple = swift.get_object(uuid, 'in.mp4')
        try:
            with open(uuid + '/in.mp4', 'w') as file:
                file.write(obj_tuple[1])
        except swiftclient.exceptions.ClientException:
            print(" [x] Transcoding aborted, no file for %r" % uuid)
            ch.basic_ack(delivery_tag = method.delivery_tag)
            return


        transcode.do(uuid + '/in.mp4', self.progress)

        #send file to Swift
        with open(uuid + '/out.mp4', 'r') as file:
            swift.put_object(uuid, 'out.mp4', contents=file.read(), content_type='video/mp4')

        ch.basic_ack(delivery_tag = method.delivery_tag) 
        print(" [x] Transcoding done for ." % uuid)

        print(" [x] Deleting input.")
        swift.delete_object(uuid, 'in.mp4')
        swift.close()

if __name__ == '__main__':
    node = ProcessingNode()
