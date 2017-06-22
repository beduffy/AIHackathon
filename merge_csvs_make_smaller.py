import pandas as pd

print 'aaaas'

#from main import col_labels, final_columns, cols_to_extract

with open('column_labels_2013+.txt') as f:
    col_labels = f.read().split('\t')
    print col_labels
    
cols_to_extract = ['GLOBALEVENTID', 'SQLDATE', 'SOURCEURL', 'ActionGeo_Lat', 'ActionGeo_Long',
                   'ActionGeo_FullName', 'ActionGeo_CountryCode']
extra_columns = ['company', 'industry', 'topic', 'sentiment']
final_columns = cols_to_extract + extra_columns


# 1. merge per month
print 'hey'
df1 = pd.read_table('data/20170620.export.CSV', header=None)
df1.columns = col_labels

df2 = pd.read_table('data/20170619.export.CSV', header=None)
df2.columns = col_labels

def merge_two_dfs(df1, df2):
    df_comb = df1.append(df2)

    print df_comb.shape, df1.shape, df2.shape
    del df1
    del df2
    df_comb = df_comb[cols_to_extract]
    print 'shape after columns', df_comb.shape
    print 'shape with duplicates:', df_comb.shape
    df_comb = df_comb.drop_duplicates(subset=['SOURCEURL'], keep='first')
    print 'shape after dropping duplicates:', df_comb.shape
    
    return df_comb

merge_two_dfs(df1, df2)
# 2. remove unneccesary columns


'''fout=open("out.csv","a")
# first file:
for line in open("sh1.csv"):
    fout.write(line)
# now the rest:    
for num in range(2,201):
    f = open("sh"+str(num)+".csv")
    f.next() # skip the header
    for line in f:
         fout.write(line)
    f.close() # not really needed
fout.close()'''