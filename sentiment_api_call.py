import requests
import json
import csv
import time
import threading

import pandas as pd

sentiment_api_url = 'http://10.69.161.21:1555/twitterRESTfulService/tweet/text/query?data='
dataset_path = 'final_dataset_v1.csv'
#dataset_path = 'merged_big_dataset_5.csv'

df = pd.read_csv(dataset_path)

if 'Unnamed: 0' in df.columns:
    df = df.drop(['Unnamed: 0'], axis=1)

print df.head()

sentiments = [""] * df.shape[0]

start = time.time()



def call_api(title, index):
    """thread worker function"""
    r = requests.get(sentiment_api_url + title)
    resp_dict = json.loads(r.text)
    sentiments[index] = (resp_dict['result'])
    print idx, resp_dict


    
threads = []
for idx, title in enumerate(df['title']):
    t = threading.Thread(target=call_api, args=(title, idx, ))
    threads.append(t)
    t.start()
    
    if len(threads) > 10:
        for i in xrange(len(threads) - 1, -1, -1):
            threads[i].join()
            del threads[i]
        time.sleep(1)
            
df['sentiment'] = sentiments

df.to_csv(dataset_path, index=False)

end = time.time()
print('Time taken:', end - start)