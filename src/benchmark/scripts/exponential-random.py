from numpy import random
from sys import argv

rate = float(argv[1])
# scale is inverse of rate
scale = 1/rate
x = random.exponential(scale=scale)
print(x)