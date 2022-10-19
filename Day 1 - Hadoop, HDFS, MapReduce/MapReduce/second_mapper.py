#!/bin/python3

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from reducer.py
    # omit the day in the customerID_date
    customerID_date, consumptionPerDay = line.split("\t", 1)
    customerID_date = customerID_date.split("-",1)[0]

    # return customerID_date as key and the consumption as value
    print("{}\t{}".format(customerID_date, consumptionPerDay))
