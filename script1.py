#Rev A 20230128 Luis Figueroa

import boto3

s3 = boto3.resource(
    service_name='s3',
    region_name='eu-central-1',
    aws_access_key_id='***', # write here the aws_access_key_id
    aws_secret_access_key='***' # write here the aws_secret_access_key
    )

bucket='apariciodev'

import requests
from bs4 import BeautifulSoup as bs

url = 'http://www.jobs.ch/en/vacancies/?term=Data%20Analyst'
page = requests.get(url)
soup = bs(page.content, "html.parser")

from datetime import datetime
import time
# Date & Time for registers
now = datetime.now()
datemed = int(now.strftime('%Y%m%d'))
timemed = int(now.strftime('%H%M'))

# path
path ="/home/ubuntu/scripts/scrapper-jobsch/data/"
folder =str(datemed)+'/'

import numpy as np
import pandas as pd
import random
results = soup.find(class_="Div-sc-1cpunnt-0 ujqkk")
job_elements = results.find_all("a", class_="Link__ExtendedRR6Link-sc-czsz28-1 jzwvjr Link-sc-czsz28-2 VacancyLink___StyledLink-sc-ufp08j-0 bzpUGN zoplL")

counter=0
arch =[]
pos =[]
job_position=[]
company=[]
data =[]
lk =[]
date =[]
print(now,'file: 1',len(job_elements))
for job_element in job_elements:
    counter += 1
    arch.append(1)
    pos.append(counter)
    job_position.append(job_element.find("span", class_="Span-sc-1ybanni-0 Text__span-sc-1lu7urs-12 Text-sc-1lu7urs-13 VacancySerpItem___StyledText-sc-ppntto-4 jpKTRn bbefum hSicAH"))
    company.append(job_element.find("p", class_="P-sc-hyu5hk-0 Text__p2-sc-1lu7urs-10 Span-sc-1ybanni-0 Text__span-sc-1lu7urs-12 Text-sc-1lu7urs-13 cHnalP cTUsVs"))
    data.append(job_element.text)
    lk.append('https://jobs.ch'+job_element.get('href'))
    date.append(now)

#df=pd.DataFrame({"page":arch,"pos_page":pos,"date":date,"job_position":job_position,"company":company,"data":data,"link":lk})
#df.to_csv(path+'job-scrapp_'+str(datemed)+'_1.csv',index=True, index_label='index')

for i in range(2, 21):
    url = "https://www.jobs.ch/en/vacancies/?page="+str(i)+"&term=Data%20Analyst"
    page = requests.get(url,url)
    soup = bs(page.content, "html.parser")
    now = datetime.now()
    print(now,'file: '+str(i),len(job_elements))
    results = soup.find(class_="Div-sc-1cpunnt-0 ujqkk")
    job_elements = results.find_all("a", class_="Link__ExtendedRR6Link-sc-czsz28-1 jzwvjr Link-sc-czsz28-2 VacancyLink___StyledLink-sc-ufp08j-0 bzpUGN zoplL")
    time_sleep =random.randint(1,3)
    time.sleep(time_sleep)

    counter=0
    #arch =[]
    #pos =[]
    #job_position=[]
    #company=[]
    #data =[]
    #lk =[]
    #date =[]
    for job_element in job_elements:
        counter += 1
        arch.append(i)
        pos.append(counter)
        job_position.append(job_element.find("span", class_="Span-sc-1ybanni-0 Text__span-sc-1lu7urs-12 Text-sc-1lu7urs-13 VacancySerpItem___StyledText-sc-ppntto-4 jpKTRn bbefum hSicAH"))
        company.append(job_element.find("p", class_="P-sc-hyu5hk-0 Text__p2-sc-1lu7urs-10 Span-sc-1ybanni-0 Text__span-sc-1lu7urs-12 Text-sc-1lu7urs-13 cHnalP cTUsVs"))
        data.append(job_element.text)
        lk.append('https://jobs.ch'+job_element.get('href'))
        date.append(now)

df=pd.DataFrame({"page":arch,"pos_page":pos,"date":date,"job_position":job_position,"company":company,"data":data,"link":lk})

df['job_pos']=df['job_position'].astype(str).str.replace(r'Span-sc-1ybanni-0 Text__span-sc-1lu7urs-12 Text-sc-1lu7urs-13 VacancySerpItem___StyledText-sc-ppntto-4 jpKTRn bbefum hSicAH', '').str.strip()
df['job_pos']=df['job_pos'].str.replace(r'</span>', '').str.strip()
df['job_pos']=df['job_pos'].str.replace(r'<span class=', '').str.strip()
df['job_pos']=df['job_pos'].str.replace(r'"">', '').str.strip()
df['job_pos']=df['job_pos'].str.replace(r'&amp;', '&').str.strip()
df['comp']=df['company'].astype(str).str.replace(r'<p class=', '').str.strip()
df['comp']=df['comp'].str.replace(r'"P-sc-hyu5hk-0 Text__p2-sc-1lu7urs-10 ', '').str.strip()
df['comp']=df['comp'].str.replace(r'Span-sc-1ybanni-0 Text__span-sc-1lu7urs-12 ', '').str.strip()
df['comp']=df['comp'].str.replace(r'Text-sc-1lu7urs-13 cHnalP cTUsVs"><strong>', '').str.strip()
df['comp']=df['comp'].str.replace(r'</strong></p>"', '').str.strip()
df['comp']=df['comp'].str.replace(r'Text-sc-1lu7urs-13 cHnalP cTUsVs">', '').str.strip()
df['comp']=df['comp'].str.replace(r'</p>', '').str.strip()
df['comp']=df['comp'].str.replace(r'</strong>', '').str.strip()
df['comp']=df['comp'].str.replace(r'&amp;', '&').str.strip()
#df['datapub']=df['data'].str.replace(r'.', '').str.strip()
df['datapub']=df['data'].str[-30:]
df['datapub']=df['datapub'].str.replace(r'blished: ', '').str.strip()
df['datapub']=df['datapub'].str.replace(r'ulished: ', '').str.strip()
df['datapub']=df['datapub'].str.replace(r'lished: ', '').str.strip()
df['datapub']=df['datapub'].str.replace(r'ished: ', '').str.strip()
m=df['datapub']

df['Jan']=m.str.contains(pat = 'Januar')
df['Feb']=m.str.contains(pat = 'Feb')
df['Mar']=m.str.contains(pat = 'Mar')
df['Apr']=m.str.contains(pat = 'Apr')
df['May']=m.str.contains(pat = 'May')
df['Jun']=m.str.contains(pat = 'Jun')
df['Jul']=m.str.contains(pat = 'Jul')
df['Aug']=m.str.contains(pat = 'Aug')
df['Sep']=m.str.contains(pat = 'Sep')
df['Oct']=m.str.contains(pat = 'O')
df['Nov']=m.str.contains(pat = 'Nov')
df['Dec']=m.str.contains(pat = 'De')
conditions = [
    (df['Jan'] ==True),
    (df['Feb'] ==True),
    (df['Mar'] ==True),
    (df['Apr'] ==True),
    (df['May'] ==True),
    (df['Jun'] ==True),
    (df['Jul'] ==True),
    (df['Aug'] ==True),
    (df['Sep'] ==True),
    (df['Oct'] ==True),
    (df['Nov'] ==True),
    (df['Dec'] ==True)]
choices = [1,2,3,4,5,6,7,8,9,10,11,12]
df['mes'] = np.select(conditions, choices, default=0)
df['id_link']=df['link'].str[37:-23]
df=df[['page','pos_page','date','job_pos','comp','datapub','mes','link','id_link']]

print(now,'Inicial',df.shape)
df.drop_duplicates(subset=['id_link'])
print(now,'Final',df.shape)
print()
file_name =path+'scrappedjobs_'+str(datemed)+'.csv'

df.to_csv(file_name,index=True, index_label='index')    

output = 'data/scrapper-jobsch/raw/scrappedjobs_'+str(datemed)+'.csv'
s3.Bucket(bucket).upload_file(Filename=file_name, Key=output)