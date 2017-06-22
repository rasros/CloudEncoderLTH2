#!/usr/bin/env python2

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import time
import random
import threading
import time
import sys

NUM_THREADS = 10
MEAN_SLEEP_SEC = 5

class ThreadInfo:
    def __init__(self, numthreads):
        self.mutex = threading.Lock()
        self.numthreads = numthreads
        self.status = [ "STARTUP" for i in range(numthreads)]
        self.count = [ 0 for i in range(numthreads)]
        self.exiting = False

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

    def pleaseDie(self):
        print("Please die")
        self.mutex.acquire()
        self.exiting = True
        self.status = [ "CLOSING" for i in range(self.numthreads) ]
        self.mutex.release()
        self.printInfo()

    def printInfo(self):
        info.mutex.acquire()
        str = ""
        for i in range(NUM_THREADS):
            str += "\033[95mThread {} {:15s} Done: {:3d}\033[0m\n".format(
                    i, info.status[i], info.count[1])
            str += "\033[{}F".format(NUM_THREADS+1)
        print(str)
        info.mutex.release()


info = ThreadInfo(NUM_THREADS)

def genWL(base, tidx):
    global info
    
    st = random.expovariate(MEAN_SLEEP_SEC)
    time.sleep(st)
    while (not info.exiting):
          genWL1(base, tidx, info)
          st = random.randint(8,10)
          time.sleep(st)

def genWL1(base, tidx, info):
    info.setStatus(tidx, "UPLOADING")

    m = MultipartEncoder(fields={ 'file' :
        ('filename', open('video2.mp4', 'rb'), 'video/mp4')})
    queueTime = time.time()
    r = requests.post(base,data = m,
            headers={'Content-Type': m.content_type})
    if r.status_code == 500:
        info.setStatus(tids, "SERVER ERROR")
        info.exiting = True
        return
    file_url = r.headers["Location"]
    r2 = requests.get(file_url + "/status")
    
    info.setStatus(tidx, r2.json()["status"])
    while ( info.setStatus(tidx, r2.json()["status"]) == "QUEUED" ):
        try:
            time.sleep(1)
        except Exception:
            pass
        if info.exiting:
            return
        r2 = requests.get(file_url + "/status")

    startTime = time.time()
    while ( info.setStatus(tidx, r2.json()["status"]) != "DONE" ):
        try:
            time.sleep(1)
        except Exception:
            pass
        if info.exiting:
            return
        r2 = requests.get(file_url + "/status")

    t = time.time()
    info.mutex.acquire()
    sys.stderr.write(file_url + "," +
            str(startTime-queueTime) + "," +
            str(t-startTime) + "," +
            str(t-queueTime) + "\n")
    info.mutex.release()
    info.incCount(tidx)


if __name__ == '__main__':
    print("Workload Generator started with %d threads and %d mean sleep time." \
            % NUM_THREADS % MEAN_SLEEP_SEC)
    print("CSV is printed to stderr and info to stdout.")
    print("Press CTRL+C to quit.")

    sys.stderr.write("URL,QueueTime,ProcessingTime,TotalTime\n")

    signal.signal(signal.SIGINT, info.pleaseDie)

    for i in range(NUM_THREADS):
        t = threading.Thread(target = genWL , 
                args = ["http://transcode.thefuturenow.se:5000/", i])
        t.start()

    while not info.exiting:

        time.sleep(1)
