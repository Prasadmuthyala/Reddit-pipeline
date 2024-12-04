from datetime import datetime

import pandas as pd


from utils.constants import CLIENT_ID, SECRET, OUTPUT_PATH, AWS_BUCKET_NAME
from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv, connect_to_s3


def reddit_pipeline(file_name:str,subreddit:str,time_filter='day',limit=None):
    #connecting to reddit instance
    instance=connect_reddit(CLIENT_ID, SECRET, 'DE')
    #extraction
    posts=extract_posts(instance,subreddit,time_filter,limit)
    if posts :
        post_df=pd.DataFrame(posts)
    else:
        return

    # transformation
    post_df = transform_data(post_df)
    # loading to csv
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(post_df, file_path)

    return file_path


def upload_s3_pipeline(ti):
    file_path = ti.xcom_pull(task_ids='reddit_extraction', key='return_value')

    s3 = connect_to_s3()
    # create_bucket_if_not_exist(s3, AWS_BUCKET_NAME)
    # upload_to_s3(s3, file_path, AWS_BUCKET_NAME, file_path.split('/')[-1])