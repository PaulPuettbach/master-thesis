import sys
import numpy
import os
import csv
print ('argument list', sys.argv)
directory = sys.argv[1]
#like this python3 generate-results.py ./generated/10-1-1


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
time_file=f"{directory}/time.txt"
with open(time_file) as f:
    lines = f.readlines()

ttc = [format_ttc(line) for line in lines]
mean = numpy.mean(ttc)
median = numpy.median(ttc)
#i also want mean error from the mean to signify spread
error = sum(map(lambda x : abs(x - mean), ttc))/len(ttc)

print(f"this is the ttc: {ttc}")
print(f"this is the mean: {mean}, the median: {median}, the mean error from the mean to show spread: {error}")

#take the mean squared error per tenant, error from is the average wait time normalized with currently pending pods
#(queue length) at the time they are meassured

#taken from https://stackoverflow.com/questions/33503993/read-in-all-csv-files-from-a-directory-using-python
#the issue is that i take the 
aggregate_file = f"{directory}/merged_output.csv"
with open(aggregate_file, 'r') as aggregate:
    aggregate_rows = list(csv.reader(aggregate))
    normalized_wait_array = []
    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv") and not file == "merged_output.csv":
                with open(f"{directory}/{file}", 'r') as f:
                    csvreader = csv.reader(f)
                    updated_rows = []
                    for row in csvreader:
                        start = float(row[0])
                        end = float(row[1])
                        elapsed_time = end - start
                        if elapsed_time == 0:
                            normalized_wait_array.append(elapsed_time)
                            row.append(str(elapsed_time))
                            updated_rows.append(row)
                            continue
                        counter = 0
                        for other_row in aggregate_rows:
                            #start or end time have to overlap (be inbetween start and end time)
                            if (float(other_row[0]) >= start and float(other_row[0]) < end) or (float(other_row[1]) > start and float(other_row[1]) <= end):
                                counter += 1
                            #start time is bigger then the end time means there cannot be further overlapp the
                            elif float(other_row[0]) >= end:
                                break
                        #counter minus one because the process counts the pod we are trying to normalize as well
                        normalized_wait_array.append(elapsed_time/(1 if counter-1==0 else counter-1))
                        row.append(str(elapsed_time/(1 if counter-1==0 else counter-1)))
                        updated_rows.append(row)
                    with open(f"{directory}/{file}", 'w', newline='') as temp:
                        writer = csv.writer(temp)
                        writer.writerows(updated_rows)

    mean_normalized_wait = numpy.mean(normalized_wait_array)

    #touple with tenant name and the the mean error from normalized wait time
    mean_error_from_normalized_wait_time_per_tenant = []

    for root,dirs,files in os.walk(directory):
        for file in files:
            if file.endswith(".csv") and not file == "merged_output.csv":
                with open(f"{directory}/{file}", 'r') as f:
                    csvreader = csv.reader(f)
                    #apprenently str arithmatic is not a thing so we have to do weird subarray calcs
                    suffix = "_times.csv"
                    tenant_name = file[:-len(suffix)]
                    counter = 0
                    aggregate_error = 0
                    for row in csvreader:
                        normalized_wait = float(row[2])
                        error = abs(normalized_wait - mean_normalized_wait)
                        aggregate_error += error
                        counter += 1
                    mean_error_from_normalized_wait_time_per_tenant.append((tenant_name, (aggregate_error/counter)))
print(f"this is the mean error from the normalized wait time per tenant and our metric for fairness: {mean_error_from_normalized_wait_time_per_tenant}")