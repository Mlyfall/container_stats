import os
import csv
from tqdm import tqdm
from dataloader.dataloader_factory import dataloader_factory
from dataloader.direction import Direction

# loading data
data_base_path = "/home/emmely/PycharmProjects/LID-DS-2021-fixed-exploit-time"
# scenario_names = os.listdir(data_base_path)
scenario_name = "CVE-2017-7529"
scenario_path = os.path.join(data_base_path, scenario_name)
dataloader = dataloader_factory(scenario_path, direction=Direction.BOTH)
data_types = [dataloader.training_data(), dataloader.validation_data(), dataloader.test_data()]

counter = 0

with open("all_syscalls_of_"+ scenario_name + ".csv", "w") as file:
    writer = csv.writer(file, delimiter=",")

    for data in tqdm(data_types, unit="datatype"):
        for recording in data:
            for syscall in recording.syscalls():
                syscall_line_items = syscall.syscall_line.split(" ")
                writer.writerow(syscall_line_items)
                counter += 1

print(f"Number of syscalls in {scenario_name}: {counter}")
