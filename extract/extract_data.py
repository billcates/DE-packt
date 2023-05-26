import requests
import pandas as pd
import yaml
import datetime
import os
import time
import boto3
import io

with open("config.yml") as f:
    fl=yaml.safe_load(f)
    cfg=fl['dev']

API_URL = "https://api.stackexchange.com/2.3"
ACCESS_TOKEN = cfg['key']

def fetch_trending_tags():
    #converting the timestamps to epoch values
    fromdate = int((pd.Timestamp.now().floor('D') - pd.DateOffset(months=1)).timestamp())
    todate = int(pd.Timestamp.now().floor('D').timestamp())

    params = {
        'site': 'stackoverflow',
        'key': ACCESS_TOKEN,
        'order': 'desc',
        'sort': 'popular',
        'pagesize': 100,
        'fromdate': fromdate,
        'todate': todate
    }

    response = requests.get(f"{API_URL}/tags", params=params)
    data = response.json()
    #print(data)

    if 'items' in data:
        tags=[tag['name'] for tag in data['items']]
        trending_tags = pd.DataFrame({'Tags': tags})
        return trending_tags
    else:
        return []


def check_emerging_technologies():
    #define the bucket name and prefix
    bucket_name='de-packt-stack-exchange-bucket'
    prefix='trending_tags'
    s3 = boto3.client('s3')

    # List all files in the bucket with the given prefix
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    all_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')] 


    # Sort the files and get the latest file
    all_files.sort()
    latest_file = all_files[-1]

    # Read the latest file from S3
    response = s3.get_object(Bucket=bucket_name, Key=latest_file)
    latest_tags = pd.read_csv(response['Body'])['Tags'].tolist()

    emerging_tags = set()

    # Read the rest of the files from S3 and update emerging_tags
    for file in all_files[:-1]:
        response = s3.get_object(Bucket=bucket_name, Key=file)
        tags = pd.read_csv(response['Body'])['Tags'].tolist()
        emerging_tags.update(tags)

    latest_tags = list(set(latest_tags) - emerging_tags)

    return list(latest_tags)

def fetch_popular_questions():
    fromdate = int((pd.Timestamp.now().floor('D') - pd.DateOffset(months=1)).timestamp())
    todate = int(pd.Timestamp.now().floor('D').timestamp())

    params = {
        'site': 'stackoverflow',
        'key': ACCESS_TOKEN,
        'order': 'desc',
        'sort': 'votes',
        'pagesize': 100,  # Number of questions to fetch
        'fromdate': fromdate,
        'todate': todate
    }

    response = requests.get(f"{API_URL}/questions", params=params)
    data = response.json()

    if 'items' in data:
        questions = []
        for item in data['items']:
            question_id = item['question_id']
            title = item['title']
            link = item['link']
            questions.append({'Question ID': question_id, 'Title': title, 'Link': link})
        
        df=pd.DataFrame(questions)
        return df
    else:
        return pd.DataFrame([])
    

def save_file(df,folder_name):
    day=datetime.datetime.now()
    df['Extract_time']=day
    dt=day.strftime("%Y%m%d")
    csv_buffer = io.StringIO()
    bucket_name='de-packt-stack-exchange-bucket'
    object_name=folder_name+'/'+folder_name+'_'+dt+'.csv'
    df.to_csv(csv_buffer, index=False)

    s3_client = boto3.client('s3')
    try:
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=object_name)
        print(f"DataFrame uploaded successfully as CSV: s3://{bucket_name}/{object_name}")
    except Exception as e:
        print(f"Error uploading DataFrame: {e}")


if __name__ == '__main__':
    st=datetime.datetime.now()
    print("start time is ",st)
    print("get the trending tags for the past month")
    trending_tags = fetch_trending_tags()
    print("upload the dataframe as csv to bucket")
    save_file(trending_tags,'trending_tags')
    emerging_technologies = check_emerging_technologies()
    print("Emerging Technologies:")
    print(emerging_technologies)
    print("Fetching Popular questions by votes")
    df_popular=fetch_popular_questions()
    print("Uploading the df_popular to s3")
    save_file(df_popular,'popular_questions')
    end=datetime.datetime.now()
    print("End time is ",end)
    print("Total Time taken is ",end-st)

