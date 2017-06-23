import requests
import json
import csv
import time

import pandas as pd

sentiment_api_url = 'http://10.69.161.21:1555/twitterRESTfulService/tweet/text/query?data='
dataset_path = 'final_dataset_v1.csv'
#dataset_path = 'merged_big_dataset_5.csv'

df = pd.read_csv(dataset_path)

if 'Unnamed: 0' in df.columns:
    df = df.drop(['Unnamed: 0'], axis=1)

print df.head()

sentiments = []

start = time.time()
for idx, title in enumerate(df['title']):
    r = requests.get(sentiment_api_url + title)
    resp_dict = json.loads(r.text)
    sentiments.append(resp_dict['result'])
    print idx, resp_dict
    
df['sentiments'] = sentiments

df.to_csv(dataset_path, index=False)

end = time.time()
print('Time taken:', end - start)