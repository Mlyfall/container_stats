import os

"""database_path = "/home/eschulze/LID-DS-2021-no-relative-time"
scenarios = os.listdir(database_path)
for scenario in scenarios:
    data_folder = os.listdir(os.path.join(database_path, scenario))
    for folder in data_folder:
        print(entry)"""

with open("src/test.csv", "r") as file:
    counter = 0
    previous = 0
    max_diff = 0
    for line in file:
        counter += 1
        if counter == 1:
            previous = int(line.split(" ")[0])
        elif counter > 1:
            diff = int(line.split(" ")[0]) - previous
            if diff > max_diff:
                max_diff = diff

    print(max_diff * 10 ** -9)
    print(counter)



