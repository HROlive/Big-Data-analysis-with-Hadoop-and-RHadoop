#!/bin/python3

from operator import itemgetter
import sys
import collections


# allConsumptions dictionary contains keys that
# denote the customer ID + year and values that 
# denote a list of the consumption of every year
allConsumptions = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from secondmapper
    customerID_date, consumptionPerDay = line.split("\t", 1)

    # convert the consumption (currently a string) to int
    try:
        consumptionPerDay = int(consumptionPerDay)
    except ValueError:
        # consumption was not a number, so silently
        # ignore/discard this line
        continue

    # we check if the customerID is already in the dictionary
    # if yes, we append the current consumption to the value list
    # else we create a new list
    if customerID_date in allConsumptions:
        allConsumptions[customerID_date].append(consumptionPerDay)
    else:
        allConsumptions[customerID_date] = []
        allConsumptions[customerID_date].append(consumptionPerDay)
    
counter = collections.Counter()

# we iterate over the whole dictionary and output the 
# consumer ID + the year as key and the average consumption per day as value
for year in allConsumptions:
    counter[year] = sum(allConsumptions[year]) / len(allConsumptions[year])
    
for top in counter.most_common(10):
    print("{}".format(top))
