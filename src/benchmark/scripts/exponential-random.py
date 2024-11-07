from numpy import random
from sys import argv

rate = argv[1]
# scale is inverse of rate
scale = 1/int(rate)
x = random.exponential(scale=scale)