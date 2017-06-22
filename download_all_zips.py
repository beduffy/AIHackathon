import urllib
import time

links = []

with open('all_files_to_download_left.txt') as all_files_txt:
    for line in all_files_txt.readlines():
        #print line
        links.append(line.rstrip())

for link in links:
    dest_path = 'data/' + link.split('/')[-1]
    urllib.urlretrieve(link, dest_path)
    print 'Downloaded link:', link, 'to', dest_path
    time.sleep(1)