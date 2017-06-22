#!/usr/bin/env python2

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import time
import random
import threading
import time
import sys

def genWL(base, id):
    while(True):
       	try:
        	genWL1(base, id)
        	st = random.randint(8,10)
        	time.sleep(st)
       	except Exception as e:
       		print(e)

def handleStatus(id, old, new):
	if old != new:
		print("Thread {} {}".format(id, new))
	return new

def genWL1(base, id):
    status = None
    print("Thread {} uploading".format(id))
    m = MultipartEncoder(fields={ 'file' :
        ('filename', open('video2.mp4', 'rb'), 'video/mp4')})
    queueTime = time.time()
    r = requests.post(base,data = m,
            headers={'Content-Type': m.content_type})
    if r.status_code == 500:
        print("500 from server")
        sys.exit(1)
    file_url = r.headers["Location"]
    r2 = requests.get(file_url + "/status")
    
    status = handleStatus(id, status, r2.json()["status"])
    while ( status == "QUEUED" ):
        status = handleStatus(id, status, r2.json()["status"])
        time.sleep(1)
    startTime = time.time()
    while ( status != "DONE" ):
        time.sleep(1)
        r2 = requests.get(file_url + "/status")
        status = handleStatus(id, status, r2.json()["status"])

#    r3 = requests.get(file_url + "/download")
#    theFile = open('tmp.mp4','wb')
#    theFile.write(r3.content)
    t = time.time()
    print(file_url + "," 
            + str(startTime-queueTime) + ","
            + str(t-startTime) + ","
            + str(t-queueTime))



if __name__ == '__main__':
    print("URL,QueueTime,ProcessingTime,TotalTime")
    for i in range(10):
        t = threading.Thread(target = genWL , args = ["http://transcode.thefuturenow.se:5000/", i])
        t.start()
        st = random.randint(1,10)
        time.sleep(st)

