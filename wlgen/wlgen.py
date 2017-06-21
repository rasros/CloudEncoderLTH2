#!/usr/bin/env python2

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import time
import random
import threading
import time

def genWL(base):
    while(True):
        genWL1(base)
        st = random.randint(8,10)
        time.sleep(st)

def genWL1(base):
    m = MultipartEncoder(fields={ 'file' :
        ('filename', open('video.mp4', 'rb'), 'video/mp4')})
    startTime = time.time()
    r = requests.post(base,data = m,
            headers={'Content-Type': m.content_type})
    file_url = r.headers["Location"]
    r2 = requests.get(file_url + "/status")
    status = r2.json()["status"]
    while ( status != "DONE" ):
        time.sleep(1)
        r2 = requests.get(file_url + "/status")
        status = r2.json()["status"]

    r3 = requests.get(file_url + "/download")
#    theFile = open('tmp.mp4','wb')
#    theFile.write(r3.content)
    print(file_url + "," + str(time.time()-startTime) )



if __name__ == '__main__':
    for i in range(1):
        t = threading.Thread(target = genWL , args = ["http://127.0.0.1:5000/"])
        t.start()
        st = random.randint(1,10)
        time.sleep(st)

