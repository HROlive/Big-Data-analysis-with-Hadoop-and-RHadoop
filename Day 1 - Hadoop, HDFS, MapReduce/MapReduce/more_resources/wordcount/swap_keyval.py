#!/bin/python3
import sys
for line in sys.stdin:
    word, count = line.strip().split('\t')
    if int(count)>100:
        print("{}\t{}".format(count, word))
