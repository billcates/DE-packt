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


def fetch_top_answered_questions(tags):
    questions = []

    for tag in tags:
        params = {
            'site': 'stackoverflow',
            'key': ACCESS_TOKEN,
            'order': 'desc',
            'sort': 'votes',
            'pagesize': 50,  # Number of questions to fetch per tag
            'tagged': tag,
            'filter': '!9_bDE(fI5'
        }

        response = requests.get(f"{API_URL}/questions", params=params)
        data = response.json()

        if 'items' in data:
            for item in data['items']:
                question_id = item['question_id']
                title = item['title']
                link = item['link']
                answer_count = item['answer_count']
                questions.append({
                    'Tag': tag,
                    'Question ID': question_id,
                    'Title': title,
                    'Link': link,
                    'Answer Count': answer_count
                })

        time.sleep(2)  # Add delay between API requests

    questions_tag = pd.DataFrame(questions)
    return questions_tag


if __name__ == '__main__':
    st=datetime.datetime.now()
    print("start time is ",st)
    tags = ['python', 'java', 'javascript']  # Specify the tags you want to analyze
    tag_questions=top_answered_questions = fetch_top_answered_questions(tags)
    save_file(tag_questions,'tag_questions')
    end=datetime.datetime.now()
    print("End time is ",end)
    print("Total Time taken is ",end-st)