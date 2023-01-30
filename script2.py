# SCRIPT2.py     Rev A 20230128 Luis Figueroa

import numpy as np
import pandas as pd
import random
from datetime import datetime
import time
# Date & Time for registers
now = datetime.now()
datemed = int(now.strftime('%Y%m%d'))
timemed = int(now.strftime('%H%M'))

import requests
from bs4 import BeautifulSoup as bs
import boto3

bucket = "apariciodev"
file_name = 'data/scrapper-jobsch/raw/scrappedjobs_'+str(datemed)+'.csv'
s3 = boto3.client(
    service_name='s3',
    region_name='eu-central-1',
    aws_access_key_id='***', # Write here the aws_access_key_id
    aws_secret_access_key='***' # Write here the aws_secret_access_key
    )

obj = s3.get_object(Bucket= bucket, Key= file_name) 

df = pd.read_csv(obj['Body'], index_col='index')

list_url=df['link']
#sample=list_url.sample(frac=0.03, replace=True, random_state=1) for testing

link=[]
job_description=[]

for url in list_url:
    page = requests.get(url)
    soup = bs(page.content, "html.parser")
    results = soup.find(class_="Div-sc-1cpunnt-0 hmYTYH")
    text=results.text
    job_description.append(text)
    link.append(url)

jobs=pd.DataFrame({"link":link,"job_description":job_description})

jobs.to_csv('/home/ubuntu/scripts/scrapper-jobsch/data/scrappedjobs_descriptions_'+str(datemed)+'.csv',index=True, index_label='index')    

file_ready = '/home/ubuntu/scripts/scrapper-jobsch/data/scrappedjobs_descriptions_'+str(datemed)+'.csv'
output = 'data/scrapper-jobsch/raw/scrappedjobs_descriptions_'+str(datemed)+'.csv'

s3 = boto3.resource(
    service_name='s3',
    region_name='eu-central-1',
    aws_access_key_id='***', # Write here the aws_access_key_id
    aws_secret_access_key='***' # Write here the aws_secret_access_key
    )
s3.Bucket(bucket).upload_file(Filename=file_ready, Key=output)

print(now,'Total Job Descriptions Scrapped: ',len(list_url))