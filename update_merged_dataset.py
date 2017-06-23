from urlparse import urlparse
import pandas as pd
pd.options.display.max_columns = 999


dataset_fp = 'merged_big_dataset_rows_13372.csv'

tech_topics_fp = 'technology_topics.txt'
health_topics_fp = 'healthcare_topics.txt'

def find_topic(title, tech_topics, health_topics):
    for topic in tech_topics:
        if topic in title:
            return topic
            
    for topic in health_topics:
        if topic in title:
            return topic
            
    return 'other'
    
def find_industry_from_topic(topic, tech_topics, health_topics):
    if topic in tech_topics:
        return 'technology'
        
    if topic in health_topics:
        return 'health'
            
    return 'other'

def get_domain(url):
    parsed_uri = urlparse(url)
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    
    domain = domain.split('/')[2]
    #print domain
    return domain
    


    
if __name__ == "__main__":
    with open(tech_topics_fp) as f:
        tech_topics = [t.lower().strip() for t in f.readlines()]
        print 'technology topics\n', tech_topics
        
    with open(health_topics_fp) as f:
        health_topics = [t.lower().strip() for t in f.readlines()]
        print 'health topics\n', health_topics

    df = pd.read_csv(dataset_fp).drop(['Unnamed: 0'],axis=1)
    print df['title'].value_counts()

    df['domain'] = df['SOURCEURL'].map(get_domain)
        
    df['topic'] = df['title'].apply(find_topic, args=(tech_topics, health_topics, ))
    df['industry'] = df['topic'].apply(find_industry_from_topic, args=(tech_topics, health_topics, ))

    print df.shape
    df_industries = df[df['industry'] != 'other']

    print df_industries.head()
    print df_industries['topic'].value_counts()
    print df_industries['industry'].value_counts()
    print df_industries.shape

    df_industries.to_csv('final_dataset_v1.csv', index=False)

