# coding=utf-8
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
from io import StringIO
import requests
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#Соединение с Clickhouse
connection_extract = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database':'simulator_20230820',
    'user':'student',
    'password':'dpo_python_2020'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'ayurina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 9, 12),
}

# Интервал запуска DAG
schedule_interval = '0 23 * * *'


# функция для DAG'а    
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_ayurina_etl():

    # функция для извлечения данных
    @task()
    def extract_feed_actions():
        query_feed = """SELECT 
                            toDate(time) as event_date, 
                            user_id as user,
                            gender, age, os,
                            countIf(action='view') AS views,
                            countIf(action='like') AS likes
                        FROM 
                            simulator_20230820.feed_actions 
                        where 
                            event_date = today() - 1
                        group by
                            event_date, user, os, gender, age
                        order by user"""
        df_feed = ph.read_clickhouse(query_feed, connection=connection_extract)
        return df_feed
    
    @task()
    def extract_message_actions():
        query_message = """SELECT 
                                event_date, user, gender, age, os,
                                messages_recieved, messages_sent,
                                users_recieved, users_sent
                            FROM
                            (SELECT 
                                toDate(time) AS event_date, 
                                user_id AS user,
                                gender, age, os,
                                count(reciever_id) AS messages_sent,
                                count(distinct reciever_id) as users_sent
                            FROM simulator_20230820.message_actions
                            GROUP BY event_date, user, gender, age, os
                            HAVING event_date = today() - 1) t1
                            JOIN
                            (SELECT 
                                toDate(time) AS event_date, 
                                reciever_id AS user,
                                count(user_id) AS messages_recieved,
                                count(distinct user_id) as users_recieved
                            FROM simulator_20230820.message_actions
                            GROUP BY event_date, user
                            HAVING event_date = today() - 1) t2
                            ON t1.user = t2.user
                            order by user"""
        df_message=ph.read_clickhouse(query_message, connection=connection_extract)
        return df_message
    
    # функция для объединения таблиц
    @task()
    def merge_data(df_feed, df_message):
        df_cube = pd.merge(df_feed, df_message, how='outer', on=['event_date', 'user', 'gender', 'age', 'os'])
        df_cube.fillna(0, inplace=True)
        return df_cube

    #срез данных по полу
    @task
    def transfrom_gender(df_cube):
        df_gender = df_cube[['event_date', 'gender', 'views', 'likes', 'users_sent', 'messages_sent', 'users_recieved', 'messages_recieved']]\
            .groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()
        df_gender['dimension'] = 'gender'
        df_gender=df_gender.rename(columns={'gender': 'dimension_value'})
        return df_gender        
    
    #срез данных по возрасту
    @task
    def transfrom_age(df_cube):
        df_age = df_cube[['event_date', 'age', 'views', 'likes', 'users_sent', 'messages_sent', 'users_recieved', 'messages_recieved']]\
            .groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()
        df_age['dimension'] = 'age'
        df_age=df_age.rename(columns={'age': 'dimension_value'})
        return df_age
    
    #срез данных по ОС
    @task
    def transfrom_os(df_cube):
        df_os = df_cube[['event_date', 'os', 'views', 'likes', 'users_sent', 'messages_sent', 'users_recieved', 'messages_recieved']]\
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        df_os['dimension'] = 'os'
        df_os=df_os.rename(columns={'os': 'dimension_value'})
        return df_os
    
    #таска на объединение срезов
    @task
    def final_table(df_gender, df_age, df_os):
        df_cube_all = pd.concat([df_gender, df_age, df_os], axis=0)
        df_cube_all = df_cube_all.astype({'dimension': 'object', 'dimension_value': 'object', 'views':'int64', 'likes':'int64', 'messages_sent':'int64', 'users_sent':'int64', 'messages_recieved':'int64', 'users_recieved':'int64' })
        df_cube_all = df_cube_all.reindex(columns=['event_date', 'dimension', 'dimension_value', 'views', 'likes', 'messages_recieved', 'messages_sent', 'users_recieved', 'users_sent'])
        return df_cube_all
    

    # Создаем таблицу в ClickHouse
    @task
    def load_to_clickhouse(df_cube_all):
        
        connection_load = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'database':'test',
            'user':'student-rw',
            'password':'656e2b0c9c'}
        
        query_load = """CREATE TABLE IF NOT EXISTS test.ayurina_etl
            (
            event_date DATE,
            dimension String,
            dimension_value String,
            views UInt64,
            likes UInt64,
            messages_recieved UInt64,
            messages_sent UInt64,
            users_recieved UInt64,
            users_sent UInt64
            ) ENGINE = MergeTree()
            ORDER BY (event_date);
            """
        ph.execute(query=query_load, connection=connection_load)
        ph.to_clickhouse(df_cube_all, 
                         table='ayurina_etl', 
                         connection=connection_load, 
                         index=False)        

    df_feed = extract_feed_actions()
    df_message = extract_message_actions()
    df_cube = merge_data(df_feed, df_message)
    df_age = transfrom_age(df_cube)
    df_gender = transfrom_gender(df_cube)
    df_os = transfrom_os(df_cube)
    df_cube_all = final_table(df_age, df_gender, df_os)
    load_to_clickhouse(df_cube_all)

dag_ayurina_etl = dag_ayurina_etl()

