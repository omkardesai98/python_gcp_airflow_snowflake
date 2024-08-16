from datetime import datetime,date
import pandas as pd
import requests
import os
from google.cloud import storage


def upload_file_to_gcs(bucket_name,destination_blob_name,source_file_name):

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)

    print(f'file(i.e {source_file_name}) get successfully uploaded to gcs location: {bucket_name}/{destination_blob_name}')
    


def fetch_data():
    url = 'https://newsapi.org/v2/everything?q=tesla&from=2024-07-16&sortBy=publishedAt&apiKey=8e41052dffa645a481669bf3de80332e'
    response = requests.get(url=url)
    data = response.json()
    df = pd.DataFrame(columns=['article_name','author','title','url','description'])

    for i in data['articles']:
        data = pd.DataFrame({
            'article_name':i['source']['name'],
            'author':['author'],
            'title':i['title'],
            'url':i['url'],
            'description':i['description']
        })
        
        df = pd.concat([df,data],ignore_index=True)
    
    current_time = datetime.date(datetime.today()).strftime('%Y%m%d%H%M%S')
    filename = f'data_{current_time}.parquet'

    # writing dataframe to parquet file
    df.to_parquet(filename)

    bucket = 'snowflake-projects'
    destination_blob_name = f'news_api_project/parquet_files/{filename}'

    upload_file_to_gcs(bucket_name=bucket,destination_blob_name=destination_blob_name,source_file_name=filename)

    # Remove local file after uploading to gcs
    os.remove(filename)




if __name__ == '__main__' :
    fetch_data()