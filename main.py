import datetime
import re
import os
from os.path import isfile, join
import urllib

import pandas as pd

pd.options.display.max_columns = 999


companies = ['accenture', 'deloitte', 'ibm', 'capgemini', ' hp ', 'infosys',
             'johnson & johnson', 'roche', 'pfizer', 'novartis', 'bayer',
             'glaxosmithkline']

cols_to_extract = ['GLOBALEVENTID', 'SQLDATE', 'SOURCEURL', 'ActionGeo_Lat', 'ActionGeo_Long',
                   'ActionGeo_CountryCode']
                   #'''"ActionGeo_FullName",''' 
extra_columns = ['company', 'industry', 'topic', 'sentiment']
final_columns = cols_to_extract + extra_columns
              
with open('column_labels_2013+.txt') as f:
    col_labels = f.read().split('\t')
    print col_labels

def clean_title(path):
    path = path.replace('-', ' ').lower()
    path = path.replace('_', ' ')
    path = path.replace('+', ' ')
    
    path = urllib.unquote(path)
    # todo domain
    # todo remove something else
    
    #path = re.sub(r"[^A-Za-z]+", '', path) # keep spaces and it'll be fine
    return path
    
def get_title(url):
    split_url = url.strip().split('/')
    
    #for i in range(len(split_url), 0, )
    if len(split_url[-1]) == 0:
        return clean_title(split_url[-2])
    return clean_title(split_url[-1])

def get_title_comp_better(url, company):
    split_url = url.strip().split('/')

    # todo remove domain
    
    for between_slash in split_url:
        if company in between_slash:
            return clean_title(between_slash)
            
    return 'not found'
    
def get_company_rows(df, company):
    df_comp = df[df['SOURCEURL'].str.contains(company)] # title or sourceurl?
    #print df_comp.head()
    #print df_acn.head(20)
    
    df_comp['title'] = df_comp['SOURCEURL'].apply(get_title_comp_better, args=(company, ))
    
    print 'Number of rows for company:', company, ':', df_comp.shape
    if df_comp.shape[0] > 0:
        print df_comp['title']
    return df_comp

    
def read_news_csv(fp):
    #df = pd.read_csv(fp)
    df = pd.read_table(fp, header=None)
    df.columns = col_labels
    print df.head()
    print df['SOURCEURL'].head()
    print 'shape with duplicates:', df.shape
    df = df.drop_duplicates(subset=['SOURCEURL'], keep='first')
    print 'shape after dropping duplicates:', df.shape
    
    df['title'] = df['SOURCEURL'].map(get_title)
    #df['title'] = df['SOURCEURL'].apply(get_title, args=(,))
    
    #print df['title']
    #print df['title'].value_counts() # very important to see error cases
    
    df_companies = pd.DataFrame(columns=final_columns)
    for idx, company in enumerate(companies):
        df_comp = get_company_rows(df, company)
        df_comp = df_comp[cols_to_extract]
        for col in extra_columns:
            df_comp[col] = ""
        df_comp['company'] = company
        
        df_companies = df_companies.append(df_comp)
    
    #print df_companies.head()
    print 'df from single file shape', df_companies.shape
    
    return df_companies
    
def get_csv_paths(num_fetch=50):
    path = 'data'   
    onlyfiles = [f for f in os.listdir(path) if isfile(join(path, f))]

    csv_file_paths = ['data/'+f for f in onlyfiles if f[-3:] == 'CSV']
    #csv_file_paths = ['data/20150420.export.CSV']    

    csv_file_paths = csv_file_paths[0:num_fetch]
    return csv_file_paths

def create_merged_dataset(num_fetch):
    dest_path = 'merged_big_dataset_{}.csv'
    csv_file_paths = get_csv_paths(num_fetch)
    df_all = pd.DataFrame(columns=final_columns)
    for fp in csv_file_paths:
        df_relevant = read_news_csv(fp)
        df_all = df_all.append(df_relevant)
        # append to large df containing everything of relevance from every csv
      
    df_all = df_all.reset_index(drop=True)
    print df_all.head()
    print 'df_all shape', df_all.shape

    df_all.to_csv(dest_path, index=True)
    
create_merged_dataset(1)
#create_merged_dataset(10)
    