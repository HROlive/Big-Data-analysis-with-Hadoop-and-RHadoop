#!/bin/python3
from operator import itemgetter
import sys

currentCustomer = None
currentConsumption = 0
consumption = None

# input comes from STDIN
for line in sys.stdin:

    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    customerID_date, consumptionPerDay = line.split("\t", 1)

    # convert consumptionPerDay (currently a string) to int
    try:
        consumptionPerDay = int(consumptionPerDay)
    except ValueError:
        # consumptionPerDay was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: customerID_date) before it is passed to the reducer
    if currentCustomer == customerID_date:
        currentConsumption += consumptionPerDay
    else:
        if currentConsumption:
            # we write the result of the previous customer to STDOUT
            print("{}\t{}".format(currentCustomer, currentConsumption))
        # we set a new currentCustomer and a new currentConsumption
        currentConsumption = consumptionPerDay
        currentCustomer = customerID_date

#printing the very last customer also to STDOUT
if currentCustomer == customerID_date:
    print("{}\t{}".format(currentCustomer, currentConsumption))
