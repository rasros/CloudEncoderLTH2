#!/usr/bin/env python2

import subprocess
import time
import string
import os
import re

def do(uuid,callback):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    p = subprocess.Popen([dir_path + '/transcode.sh', uuid])
    progress = -1
    noprogress = 0
    print(" [x] Starting transcoding of UUID %r" % uuid)
    while progress < 99 and noprogress < 60:
        time.sleep(1)
        try:
            line = subprocess.check_output(['tail', '-1', str(uuid) + '/stdout.txt'])
            printable = set(string.ascii_letters+string.digits+':% \n')
            line = filter(lambda x: x in printable, line)
            ix = line.rfind('Pos:')
            pos = line[ix:]
            m = re.match('Pos: [0-9 ]+s [0-9 ]+f *([0-9]+)%', pos)
            if m:
                progress = int(m.group(1).strip())
                callback(uuid,progress)
                print(' [x] Converted %d%%.' % progress)
                noprogress = 0
            else:
                noprogress = noprogress+1
        except subprocess.CalledProcessError:
            # nothing to do
            noprogress = noprogress+1
            pass

    callback(uuid,100)
    print(" [x] Awaiting termination of mencoder.")
    p.wait()

