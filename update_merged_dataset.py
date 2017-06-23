import pandas as pd
pd.options.display.max_columns = 999

dataset_fp = 'merged_big_dataset_rows_13372.csv'

tech_topics_fp = 'technology_topics.txt'
health_topics_fp = 'healthcare_topics.txt'

with open(tech_topics_fp) as f:
    tech_topics = f.read().lower().split()
    print tech_topics
    
with open(health_topics_fp) as f:
    health_topics = f.read().lower().split()
    print health_topics
    
df = pd.read_csv(dataset_fp)

print df.head()

#print df['title']

print df['title'].value_counts()

def find_topic(title, topics):
    for topic in topics:
        if topic in title:
            return topic
            
    return 'no topic'
    
def find_industry_from_topic(topic, topics, industry):
    if topic in topics:
        return industry
            
    return 'no industry'


df['topic'] = df['title'].apply(find_topic, args=(tech_topics, ))
df['topic'] = df['title'].apply(find_topic, args=(health_topics, ))

df['industry'] = df['topic'].apply(find_industry_from_topic, args=(tech_topics, 'technology', ))
df['industry'] = df['topic'].apply(find_industry_from_topic, args=(health_topics, 'health', ))

print df.shape
df_industries = df[df['industry'] != 'no industry']

print df_industries.head()
print df_industries['topic'].value_counts()
print df_industries['industry'].value_counts()

print df_industries.shape

