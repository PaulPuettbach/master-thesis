import sys
import numpy
print ('argument list', sys.argv)
diretory = sys.argv[1]

#take the mean squared error per tenant, error from is the average wait time normalized with currently pending pods
#(queue length) at the time they are meassured

def format_ttc(time_str):
    time_str = time_str.strip()
    if not time_str:
        return None
    minutes, rest = time_str.split('m')
    seconds, milliseconds = rest.split('.')
    milliseconds= milliseconds.rstrip('s')
    total_ms = ((int(minutes) * 60 + int(seconds)) * 1000) + int(milliseconds)
    return total_ms

""""-------------------------------this is the start of the script-------------------------------"""
time_file=f"{diretory}/time.txt"
with open(time_file) as f:
    lines = f.readlines()

ttc = [format_ttc(line) for line in lines]
mean = numpy.mean(ttc)
median = numpy.median(ttc)
#i also want mean error from the mean to signify spread
error = sum(map(lambda x : abs(x - mean), ttc))/len(ttc)

print(f"this is the ttc: {ttc}")
print(f"this is the mean: {mean}, the median: {median}, the mean error from the mean to show spread: {error}")