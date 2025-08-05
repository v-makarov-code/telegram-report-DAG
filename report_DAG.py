import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import pandahouse as ph

q_DAU = """
SELECT toDate(time) AS day,
       count(DISTINCT user_id) AS users
  FROM simulator_20250520.feed_actions
 WHERE day BETWEEN today()-7
   AND today()-1
 GROUP BY day
"""
q_DAU_y = """
SELECT toDate(time) AS day,
       count(DISTINCT user_id) AS users
  FROM simulator_20250520.feed_actions
 WHERE day = today()-1
 GROUP BY day
"""
q_views = """
SELECT toDate(time) AS day,
       sum(action='view') AS views
  FROM simulator_20250520.feed_actions
 WHERE day BETWEEN today()-7
   AND today()-1
 GROUP BY toDate(time)
"""
q_views_y = """
SELECT toDate(time) AS day,
       sum(action='view') AS views
  FROM simulator_20250520.feed_actions
 WHERE day = today()-1
 GROUP BY toDate(time)
"""
q_likes = """
SELECT toDate(time) AS day,
       sum(action='like') AS likes
  FROM simulator_20250520.feed_actions
 WHERE day BETWEEN today()-7
   AND today()-1
 GROUP BY toDate(time)
"""
q_likes_y = """
SELECT toDate(time) AS day,
       sum(action='like') AS likes
  FROM simulator_20250520.feed_actions
 WHERE day = today()-1
 GROUP BY toDate(time)
"""
q_ctr = """
SELECT toDate(time) AS day,
       countIf(user_id, action='like')/countIf(user_id, action='view') AS ctr
  FROM simulator_20250520.feed_actions
 WHERE day BETWEEN today()-7
   AND today()-1
 GROUP BY toDate(time)
"""
q_ctr_y = """
SELECT toDate(time) AS day,
       countIf(user_id, action='like')/countIf(user_id, action='view') AS ctr
  FROM simulator_20250520.feed_actions
 WHERE day = today()-1
 GROUP BY toDate(time)
"""
my_token = '7244644521:AAHOiP8zKPaNYRkCnnd9uHPWNkZ-fYcis5g'
bot = telegram.Bot(token=my_token)
chat_id = -1002614297220

default_args = {
    'owner': 'v.makarov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 6, 26)
}

connection = {'host': 'https://clickhouse.lab.karpov.courses',
'database':'simulator_20250520',
'user':'student',
'password':'dpo_python_2020'
}

def get_report():
    # Отправка сообщения
    # DAU
    DAU_yesterday = ph.read_clickhouse(q_DAU_y, connection=connection)
    date = DAU_yesterday.to_numpy()[0, 0].date()
    DAU = DAU_yesterday.to_numpy()[0, 1]
    # views 
    views_yesterday = ph.read_clickhouse(q_views_y, connection=connection)
    views = views_yesterday.to_numpy()[0, 1]
    # likes 
    likes_yesterday = ph.read_clickhouse(q_likes_y, connection=connection)
    likes = likes_yesterday.to_numpy()[0, 1]
    # ctr 
    ctr_yesterday = ph.read_clickhouse(q_ctr_y, connection=connection)
    ctr = round(ctr_yesterday.to_numpy()[0, 1], 2)
    msg = f"""
    Дата: {date}\nDAU: {DAU}\nctr: {ctr}\nlikes: {likes}\nviews: {views}"""
    
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    # Отправка изображения
    DAU = ph.read_clickhouse(q_DAU, connection=connection)
    views = ph.read_clickhouse(q_views, connection=connection)
    likes = ph.read_clickhouse(q_likes, connection=connection)
    ctr = ph.read_clickhouse(q_ctr, connection=connection)
    
    fig, axes = plt.subplots(2, 2, figsize=(10, 8))
    sns.lineplot(data=DAU, x='day', y='users', ax=axes[0,0])
    axes[0, 0].set_title('DAU за 7 дней')
    sns.lineplot(data=ctr, x='day', y='ctr', ax=axes[0,1])
    axes[0, 1].set_title('CTR за 7 дней')
    sns.lineplot(data=likes, x='day', y='likes', ax=axes[1,0])
    axes[1, 0].set_title('Лайки за 7 дней')
    sns.lineplot(data=views, x='day', y='views', ax=axes[1,1])
    axes[1, 1].set_title('Просмотры за 7 дней')

    for ax in axes.flat:
        ax.tick_params(axis='x', rotation=45) 
    plt.tight_layout()

    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'report.png'
    plt.close()

    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

@dag(default_args=default_args, catchup=False, schedule_interval='0 11 * * *')
def vladislav_makarov_bxs7496_report_DAG():
    @task()
    def make_report():
        get_report()
        
    make_report()
vladislav_makarov_bxs7496_report_DAG = vladislav_makarov_bxs7496_report_DAG()