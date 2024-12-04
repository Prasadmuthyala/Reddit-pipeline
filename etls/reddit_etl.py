import sys
from os import access

import numpy as np
import pandas as pd
import requests
import json
from requests.auth import HTTPBasicAuth

from utils.constants import POST_FIELDS, AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY, AWS_REGION
import boto3
from botocore.exceptions import NoCredentialsError
# Print the Python executable used by Airflow
print("Python executable used by Airflow:", sys.executable)


def connect_reddit(client_id, client_secret, user_agent):
    # Reddit API endpoint for OAuth token
    url = 'https://www.reddit.com/api/v1/access_token'

    # Set up the necessary headers and payload for authentication
    auth = HTTPBasicAuth(client_id, client_secret)
    data = {'grant_type': 'client_credentials'}
    headers = {'User-Agent': user_agent}

    # Make the request to get the token
    response = requests.post(url, auth=auth, data=data, headers=headers)

    if response.status_code != 200:
        print(f"Error: Unable to get access token (status code: {response.status_code})")
        sys.exit(1)

    # Parse the response to get the access token
    token_data = response.json()
    access_token = token_data['access_token']

    return access_token


def extract_posts(instance,subreddit: str, time_filter: str, limit=10):
    # Get OAuth token
    access_token = instance

    # Set up the headers with the Bearer token
    headers = {
        'Authorization': f'Bearer {access_token}',
    }

    # Construct the Reddit API URL for the given subreddit and time filter
    url = f"https://oauth.reddit.com/r/{subreddit}/top.json?t={time_filter}&limit={limit}"

    # Make the GET request to the Reddit API
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: Unable to fetch data from Reddit (status code: {response.status_code})")
        return []

    # Parse the JSON response
    posts_data = response.json()

    post_lists = []
    for post in posts_data['data']['children']:
        post_info = post['data']
        print(post_info)

        post = {key: post_info[key] for key in POST_FIELDS if key in post_info}  # Filter based on POST_FIELDS

        post_lists.append(post)

    return post_lists

def transform_data(post_df: pd.DataFrame):
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)

    return post_df


def load_data_to_csv(data: pd.DataFrame, path: str):
    data.to_csv(path, index=False)

#aws
def connect_to_s3():
    try:
        s3 = boto3.client('s3',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_ACCESS_KEY,region_name=AWS_REGION)
        return s3
    except NoCredentialsError:
        print("Credentials not available.")
        return None

def create_bucket_if_not_exist(s3, bucket: str):
    try:
        # Check if the bucket exists
        response = s3.list_buckets()
        print(response)
        if bucket not in [b['Name'] for b in response['Buckets']]:
            s3.create_bucket(Bucket=bucket)
            print(f"Bucket {bucket} created")
        else:
            print(f"Bucket {bucket} already exists")
    except Exception as e:
        print(e)

def upload_to_s3(s3, file_path: str, bucket: str, s3_file_name: str):
    try:
        # Upload the file
        s3.upload_file(file_path, bucket, f'raw/{s3_file_name}')
        print(f'File uploaded to s3://{bucket}/raw/{s3_file_name}')
    except FileNotFoundError:
        print('The file was not found')
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"Error uploading file: {e}")


