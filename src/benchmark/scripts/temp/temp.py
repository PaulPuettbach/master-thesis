import sys
import os
import csv
print ('argument list', sys.argv)
directory = sys.argv[1]
output_file = 'merged_output.csv'
# List to store rows from all CSVs
all_rows = []

# Open the output file once for writing
with open(output_file, 'w', newline='') as out_csv:
    writer = None

    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(".csv"):
                file_path = os.path.join(root, file)
                print(f"this is the file {file}")
                with open(file_path, 'r', newline='') as f:
                    csvreader = csv.reader(f)
                    writer = csv.writer(out_csv)
                    for row in csvreader:
                        writer.writerow(row)

print(f"CSV files merged into {output_file}")
