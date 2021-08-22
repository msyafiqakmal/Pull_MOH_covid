from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import yaml 
import requests
import json


# Following are defaults which can be overridden later on
default_args = {
    'owner': 'M Syafiq Akmal',
    'depends_on_past': False,
    'start_date': datetime(2021, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('Pull_MOH_Covid', 
    schedule_interval='0 20 * * *',
    default_args=default_args,
    tags=['Production']
)

# Initialized Global Variables
parent_git = 'https://api.github.com/repos/MoH-Malaysia/covid19-public/contents'
all_git_filename = []
all_git_url = [] 
root_var = Variable.get("root")+"Pull_MOH_covid/"

#Utility Functions
def get_list(git_url):
    global parent_git
    global all_git_filename
    global all_git_url

    resp_contents = requests.get(git_url)
    print(resp_contents.json())
    for x in range(len(resp_contents.json())):
        filename = resp_contents.json()[x]["name"]
        if ".csv" in filename:
            all_git_url.append(resp_contents.json()[x]["download_url"])
            all_git_filename.append(filename)
        elif "." in filename: 
            continue
        else :
            get_list(parent_git+"/"+filename)


#Main Functions
def extract():
    get_list(parent_git)
    with open(root_var+'allcsvs.json', 'w') as fp:
        json.dump(dict(zip(all_git_filename, all_git_url)), fp)

def load():
    #Initialized Connection
    with open(root_var+'dbconinfo.yaml') as file:
        db_con_info = yaml.load(file, Loader=yaml.FullLoader)

    dbms        = db_con_info['dbms']
    username    = db_con_info['username']
    password    = db_con_info['password']
    server      = db_con_info['server']
    port        = db_con_info['port']
    database    = db_con_info['database']
    engine      = create_engine('{}://{}:{}@{}:{}/{}'.format(dbms,username,password,server,port,database))

    with open(root_var+'allcsvs.json', 'r') as fp:
        all_git_csv = json.load(fp)

    for filename,path in all_git_csv.items():
        pd.read_csv(path).to_sql(filename[:-4], engine,schema='covid', if_exists='replace')


t1 = PythonOperator(
    task_id='extract_source_list_git',
    python_callable=extract,
    dag=dag)

t2 = PythonOperator(
    task_id='load_to_dbms',
    python_callable=load,
    dag=dag)

t2.set_upstream(t1)
