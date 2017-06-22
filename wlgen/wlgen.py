#!/usr/bin/env python2

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import time
import random
import threading
import time
import sys

NUM_THREADS = 10

class ThreadInfo:
	def __init__(self, numthreads):
		self.mutex = threading.Lock()
		self.status = [ "Not started" for i in range(numthreads)]
		self.count = [ 0 for i in range(numthreads)]

	def setStatus(self, idx, status):
		self.mutex.acquire()
		self.status[idx] = status
		self.mutex.release()
 		return status

	def incCount(self, idx):
		self.mutex.acquire()
		cnt = self.count[idx] + 1
		self.count[idx] = cnt
	 	self.mutex.release()
	 	return cnt

	def getStatus(self, idx, status):
		self.mutex.acquire()
		status = self.status[idx]
		self.mutex.release()
		return status

info = ThreadInfo(NUM_THREADS)

def genWL(base, tidx):
    global info
    
    st = random.randint(1,10)
    time.sleep(st)
    while(True):
      	genWL1(base, tidx, info)
       	st = random.randint(8,10)
       	time.sleep(st)

def genWL1(base, tidx, info):
    info.setStatus(tidx, 'UPLOADING')

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
    
    info.setStatus(tidx, r2.json()["status"])
    while ( info.setStatus(tidx, r2.json()["status"]) == "QUEUED" ):
        time.sleep(1)

    startTime = time.time()
    while ( info.setStatus(tidx, r2.json()["status"]) != "DONE" ):
        time.sleep(1)
        r2 = requests.get(file_url + "/status")

#    r3 = requests.get(file_url + "/download")
#    theFile = open('tmp.mp4','wb')
#    theFile.write(r3.content)
    print(file_url + "," + str(startTime-queueTime) + "," + str(time.time()-startTime))
    info.incCount(tidx)

if __name__ == '__main__':
    print("URL,QueueTime,ProcessingTime")
    for i in range(NUM_THREADS):
        t = threading.Thread(target = genWL , args = ["http://transcode.thefuturenow.se:5000/", i])
        t.start()

    while True:
				info.mutex.acquire()
				str = ""
				for i in range(NUM_THREADS):
						str += "\033[95mThread {} {:15s} Done: {:3d}\033[0m\n".format(i, info.status[i], info.count[1])
			 	str += "\033[{}F".format(NUM_THREADS+1)
				print(str)
				info.mutex.release()
				time.sleep(1)
