#!/usr/bin/env python2

import sys,os

def encode():
    vargs = '-ovc x264 -x264encopts crf=20 -vf scale=1920:1080'
    oargs = '-oac lavc -lavcopts abitrate=256 -srate 48000 -channels 2'
    infile = 'mencoder/sample-Elysium.2013.2160p.mkv'
    outfile = 'mencoder/sample-Elysium.2013.2160p.mpg'
    cmd = 'mencoder ' + infile + ' -o ' + outfile + ' ' + vargs + ' ' + oargs
    os.system(cmd + ' 1> output.log 2> error.log')

if __name__ == '__main__':
    encode()
