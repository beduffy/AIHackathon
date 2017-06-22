import pandas as pd

print 'aaaas'

from main import col_labels, final_columns, cols_to_extract, get_csv_paths

'''with open('column_labels_2013+.txt') as f:
    col_labels = f.read().split('\t')
    print col_labels
    
cols_to_extract = ['GLOBALEVENTID', 'SQLDATE', 'SOURCEURL', 'ActionGeo_Lat', 'ActionGeo_Long',
                   'ActionGeo_FullName', 'ActionGeo_CountryCode']
extra_columns = ['company', 'industry', 'topic', 'sentiment']
final_columns = cols_to_extract + extra_columns'''


# 1. merge per month
def merge_two_dfs(df1, df2):
	
    df_comb = df1.append(df2)

    print df_comb.shape, df1.shape, df2.shape
    #del df1
    #del df2
    df_comb = df_comb[cols_to_extract]
    print 'shape after specific columns also with duplicates', df_comb.shape
    #print 'shape:', df_comb.shape
    df_comb = df_comb.drop_duplicates(subset=['SOURCEURL'], keep='first')
    print 'Final combined shape after dropping duplicates:', df_comb.shape
    
    return df_comb

'''print 'hey'
df1 = pd.read_table('data/20170620.export.CSV', header=None)
df1.columns = col_labels

df2 = pd.read_table('data/20170619.export.CSV', header=None)
df2.columns = col_labels'''
	
csv_file_paths = get_csv_paths(None)
	
#print csv_file_paths

for year in ['2016']:#['2016', '2017']:
	for month in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']: # 2017 ['02', '03', '04', '05', '06']:
		year_month = '{0}{1}'.format(year, month)
		print year_month
		csv_for_specific = [f for f in csv_file_paths if year_month in f]
		
		print 'num files in month:', len(csv_for_specific)
		print 'csv specific'
		print csv_for_specific
		
		csv_path_first = csv_for_specific[0]
		csv_for_specific = csv_for_specific[1:]
		
		df_all = pd.read_table(csv_path_first, header=None)
		df_all.columns = col_labels
		for csv_path in csv_for_specific:
			df_next = pd.read_table(csv_path, header=None)
			df_next.columns = col_labels
			df_all = merge_two_dfs(df_all, df_next)
			
		df_all = df_all.reset_index(drop=True)
		csv_path = 'data/merged_csvs/{}.csv'.format(year_month)
		df_all.to_csv(csv_path, index=True)
		print "\n Saved merged csv to {}".format(csv_path)

print

	
#merge_two_dfs(df1, df2)
# 2. remove unneccesary columns