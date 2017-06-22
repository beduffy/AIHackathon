import datetime

import pandas as pd

pd.options.display.max_columns = 999


with open('column_labels_2013+.txt') as f:
    col_labels = f.read().split('\t')
    print col_labels

def clean_title(path):
    return path.replace('-', ' ')
    
def get_title(url):
    split_url = url..strip().split('/')
    if len(split_url[-1]) == 0:
        return clean_title(split_url[-2])
    return clean_title(split_url[-1])

def get_company_rows(df, company):
    df_comp = df[df['SOURCEURL'].str.contains(company)] # title or sourceurl?
    #print df_comp.head()
    #print df_acn.head(20)
    print df_comp['title']
    print df_comp.shape
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
    #print df['title']
    #print df['title'].value_counts() # very important to see error cases
    
    df_acn = get_company_rows(df, 'accenture')
    df_deloitte = get_company_rows(df, 'deloitte')
    #etc
    
    # append all company dfs together
    
    # return specific 6-12 columns
    
    
csv_file_paths = ['data/20150420.export.CSV']    

for fp in csv_file_paths:
    df_relevant = read_news_csv(fp)
    
    # append to large df containing everything of relevance from every csv
    
# create large csv for dashboard
    

