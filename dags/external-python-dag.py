from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
from sqlalchemy import create_engine

def getSIDRATable(url):
    
    #Getting data from API
    response = requests.get(url).json()
    
    #Converting the response to dataframe
    df = pd.DataFrame(response)
    df.columns = df.iloc[0]
    df.drop(0, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    #Standardizing columns names
    columnList = []
    for i in range(len(df.columns)):
        tempString = df.columns[i].replace(" ", "_")
        tempString = tempString.lower()
        tempString = tempString.replace("(código)","cod")
        tempString = tempString.replace("á","a")
        tempString = tempString.replace("é","e")
        tempString = tempString.replace("í","i")
        tempString = tempString.replace("ó","o")
        tempString = tempString.replace("ú","u")
        tempString = tempString.replace("ã","a")
        tempString = tempString.replace("õ","o")
        tempString = tempString.replace("â","a")
        tempString = tempString.replace("ê","e")
        tempString = tempString.replace("î","i")
        tempString = tempString.replace("ô","o")
        tempString = tempString.replace("û","u")
        tempString = tempString.replace("ç","c")
        tempString = tempString.replace("(","")
        tempString = tempString.replace(")","")
        columnList.append(tempString)
    df.columns = columnList
    
    return df

def getBACENTable(url):
    #Getting data from API
    response = requests.get(url).json()
    
    #Converting the response to dataframe
    df = pd.DataFrame(response)
    
    #Standardizing columns names
    columnList = []
    for i in range(len(df.columns)):
        tempString = df.columns[i].replace(" ", "_")
        tempString = tempString.lower()
        tempString = tempString.replace("(código)","cod")
        tempString = tempString.replace("á","a")
        tempString = tempString.replace("é","e")
        tempString = tempString.replace("í","i")
        tempString = tempString.replace("ó","o")
        tempString = tempString.replace("ú","u")
        tempString = tempString.replace("ã","a")
        tempString = tempString.replace("õ","o")
        tempString = tempString.replace("â","a")
        tempString = tempString.replace("ê","e")
        tempString = tempString.replace("î","i")
        tempString = tempString.replace("ô","o")
        tempString = tempString.replace("û","u")
        tempString = tempString.replace("ç","c")
        tempString = tempString.replace("(","")
        tempString = tempString.replace(")","")
        columnList.append(tempString)
    df.columns = columnList
    
    return df

def dataframeToPostgreTable(user, pwd, host, port, dbname, tbname, schema, dataframe):
    conn_string = "postgresql://{0}:{1}@{2}:{3}/{4}".format(user, pwd, host, port, dbname)
    #conn_string = 'airflow:airflow@postgres/airflow'
    engine = create_engine(conn_string)
    tbname = tbname.lower() #For compatibility issues
    dataframe.to_sql(tbname, engine, schema=schema, if_exists='replace', index=False)
    print("{} - Table created with success.".format(tbname))
    
# API url list
urlList = {'basecnt': 'https://apisidra.ibge.gov.br/values/t/5932/n1/all/v/6561/p/all/c11255/90687,90691,90696,90707/d/v6561%201',
           'baseipca': 'https://apisidra.ibge.gov.br/values/t/6691/n1/all/v/2266/p/all/d/v2266%2013',
           'basepmc1': 'https://apisidra.ibge.gov.br/values/t/3415/n1/all/v/all/p/all/c11046/40311/d/v1194%201,v1195%201',
           'basepmc2': 'https://apisidra.ibge.gov.br/values/t/3416/n1/all/v/all/p/all/c11046/40311/d/v564%201,v565%201',
           'basepmc3': 'https://apisidra.ibge.gov.br/values/t/3420/n1/all/v/all/p/all/c11046/40311/d/v568%201,v569%201',
           'basepimpfbr': 'https://apisidra.ibge.gov.br/values/t/3653/n1/all/v/3136/p/all/c544/129314,129315,129316/d/v3136%201',
           'basepms': 'https://apisidra.ibge.gov.br/values/t/6443/n1/all/v/all/p/all/c11046/40311/c12355/107071/d/v8676%201,v8677%201',
           'basebacen': 'https://api.bcb.gov.br/dados/serie/bcdata.sgs.24363/dados?formato=json'
          }

def execList():
    # Creating tables in the Postgre database
    for url in urlList:
        user = "postgres"
        pwd = "hua16734958*"
        host = "192.168.0.101"
        port = "5432"
        dbname = "dbBase"
        tbname = url
        schema = "public"
        if (url == 'basebacen'):
            dataframe = getBACENTable(urlList[url])
        else:
            dataframe = getSIDRATable(urlList[url])
        dataframeToPostgreTable(user, pwd, host, port, dbname, tbname, schema, dataframe)

with DAG(
        "sidra_to_postgre_dag",
        start_date=datetime(2021,11,28),
        schedule_interval="@daily",
        #schedule_interval='*/10 * * * *', #every 10 minutes
        catchup=False
        ) as dag:
    updateSidraPostgre = PythonOperator(
        task_id='update_sidra_postgre',
        python_callable=execList
        )