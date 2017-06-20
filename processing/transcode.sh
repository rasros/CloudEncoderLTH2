#!/bin/bash

mencoder "$1/in.mp4" -o "$1/out.mp4" '-ovc' 'x264' '-x264encopts' 'crf=20' '-vf' 'scale=1920:1080' '-oac' 'lavc' '-lavcopts' 'abitrate=256' '-srate' '48000' '-channels' '2' 1> "$1/stdout.txt" 2> "$1/stderr.txt"
