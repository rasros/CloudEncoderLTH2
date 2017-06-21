#!/bin/bash
mencoder "$1/in.mp4" -o "$1/out.mp4" '-ovc' 'x264' '-x264encopts' 'crf=20' '-vf' 'scale=1080:720' '-oac' 'copy' -fafmttag 0x706D -of 'lavf' 1> "$1/stdout.txt" 2> "$1/stderr.txt"
