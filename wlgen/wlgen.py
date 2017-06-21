#!/usr/bin/env python2

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import time
import random
import threading
import time
import sys

def genWL(base):
    while(True):
        genWL1(base)
        st = random.randint(8,10)
        time.sleep(st)

def genWL1(base):
    m = MultipartEncoder(fields={ 'file' :
        ('filename', open('video.mp4', 'rb'), 'video/mp4')})
    queueTime = time.time()
    r = requests.post(base,data = m,
            headers={'Content-Type': m.content_type})
    if r.status_code == 500:
        print("500 from server, down?")
        sys.exit(1)
    file_url = r.headers["Location"]
    r2 = requests.get(file_url + "/status")
    status = r2.json()["status"]
    
    while ( status == "QUEUED" ):
        time.sleep(1)
    startTime = time.time()
    while ( status != "DONE" ):
        time.sleep(1)
        r2 = requests.get(file_url + "/status")
        status = r2.json()["status"]

#    r3 = requests.get(file_url + "/download")
#    theFile = open('tmp.mp4','wb')
#    theFile.write(r3.content)
    print(file_url + "," str(startTime-queueTime) + "," + str(time.time()-startTime))



if __name__ == '__main__':
    print("URL,QueueTime,ProcessingTime")
    for i in range(10):
        t = threading.Thread(target = genWL , args = ["http://transcode.thefuturenow.se:5000/"])
        t.start()
        st = random.randint(1,10)
        time.sleep(st)

