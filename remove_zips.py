import os
from os.path import isfile, join

path = 'data'   
onlyfiles = [f for f in os.listdir(path) if isfile(join(path, f))]

csvs = [f for f in onlyfiles if f[-3:] == 'CSV']

zips_to_delete = []
for csv in csvs:
    
    for file in onlyfiles:
        if csv in file and 'zip' in file:
            zips_to_delete.append(file)

print zips_to_delete

for f_to_delete in zips_to_delete:
    os.remove('data/' + f_to_delete)

#print csvs

#print onlyfiles