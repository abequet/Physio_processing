#Script written by A.J.Béquet during his post-doctoral work in Universidad de Salamanca (2023). 
#Export the data acquired from Empatica's EmbracePlus device (EDA, BVP, Manual tags) into a .csv format
#For use : firsty download the raw data from Empatica's server and store it in parent_dir

from avro.datafile import DataFileReader
from avro.io import DatumReader
import matplotlib.pyplot as plt
from datetime import datetime
import json
import numpy
import csv
import os
import glob

# Set the parent directory path
parent_dir = r"C:\empatica\"  # using raw-string for Windows path

# Define the file extension you're looking for
file_extension = "*.avro"

# Use the glob module to recursively find all files with the specified extension
avro_files = glob.glob(os.path.join(parent_dir, "**", file_extension), recursive=True)

# Iterate through each avro file found
for avro_file in avro_files:
    print("Processing file:", avro_file)

    ## Retrieve the timestamp from the file name
    last_underscore_index = avro_file.rfind("_")
    last_dot_index = avro_file.rfind(".")
    number_string = avro_file[last_underscore_index + 1 : last_dot_index]
    timestamp_global = [int(number_string) * 1000]

    # Create the name to record the timestamp CSV
    file_location_timestamp = avro_file.replace(".avro", "-timestampdeb.csv")
    with open(file_location_timestamp, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp_begin'])
        writer.writerows([timestamp_global])

    ## Read Avro file
    reader = DataFileReader(open(avro_file, "rb"), DatumReader())
    schema = json.loads(reader.meta.get('avro.schema').decode('utf-8'))
    data = None
    for datum in reader:
        data = datum
    reader.close()

    # Access UTC timezone (convert to milliseconds)
    timezone = data["timezone"] * 1000  # Delta [s] from UTC, multiplied to ms

    # Access Manual tags
    manualtag = data["rawData"]["tags"]
    manual_tag_micro = manualtag["tagsTimeMicros"]

    # Write manual trigger data into CSV file
    file_location_manualTAG = avro_file.replace(".avro", "-MANUALTAG.csv")
    with open(file_location_manualTAG, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['manual_trigger_timestamp'])
        writer.writerows(zip(manual_tag_micro))

    ## Process EDA Data
    eda = data["rawData"]["eda"]
    sampling_freq = eda["samplingFrequency"]
    n_samples = len(eda["values"])
    # Create the list of timestamps (assuming timestampStart is in seconds)
    eda_time = [(i / sampling_freq) + eda["timestampStart"] for i in range(n_samples)]
    combined_data_eda = list(zip(eda_time, eda["values"]))
    file_location_EDA = avro_file.replace(".avro", "-EDA.csv")
    with open(file_location_EDA, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['eda_timestamp', 'eda_values'])
        writer.writerows(combined_data_eda)

    ## Process BVP (ECG) Data
    cardiac = data["rawData"]["bvp"]
    print("Cardiac fields:")
    for key, val in cardiac.items():
        print("- " + key)

    sampling_freq_cardiac = cardiac["samplingFrequency"]
    n_samples_cardiac = len(cardiac["values"])
    # Convert timestampStart to milliseconds (if original is in microseconds, adjust accordingly)
    startMilliseconds_cardiac = cardiac["timestampStart"] / 1000
    # Create the timestamp vector in milliseconds
    time_ms_cardiac = [(i / sampling_freq_cardiac * 1000) + startMilliseconds_cardiac for i in range(n_samples_cardiac)]
    combined_data_cardiac = list(zip(time_ms_cardiac, cardiac["values"]))
    file_location_BVP = avro_file.replace(".avro", "-BVP.csv")
    with open(file_location_BVP, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['bvp_timestamp', 'bvp_value'])
        writer.writerows(combined_data_cardiac)

    print('Participant done:', avro_file)
