#!/bin/python3
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(",")

    # key: constructing the customerID_date key by concatenating the
    # value of the last element (column) which is the ID and the 
    # fourth element which is the date
    # value: the consumption of a time interval in a day, which is 
    # seventh element
    customerID_date= words[-1] + "_" + words[3]
    consumptionPerDay = words[6]

    # printing the key and the value to STDOUT
    # this line will be sorted before passed to reducer
    print("{}\t{}".format(customerID_date, consumptionPerDay))
