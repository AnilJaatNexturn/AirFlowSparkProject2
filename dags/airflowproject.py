from datetime import timedelta,datetime
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from airflow.operators.email_operator import EmailOperator

default_args={
        'owner':'aniljat',
        'start_date':datetime(2023,5,5),
        'retries':3,
        'retry_delay':timedelta(minutes=5)
        }

dags=DAG(
        'sparkwithairflow',
        default_args=default_args,
        description='snalysis with saprk airflow',
        schedule_interval='* * * * *',
        catchup=False,
        tags=['example,spark analysis']
        )



def process_data():
    spark=SparkSession.builder.appName("Spark Dataframe").getOrCreate()
    df = spark.read.csv('/root/airflow/inputfiles/timberland_stock.csv',header=True)
    df.createOrReplaceTempView("table1")
    # What is the mean of the Close column?
    df1=spark.sql("SELECT AVG(Close) AS mean_close FROM table1")
    df1.write.csv("/root/airflow/outputfiles/df1.csv", header=True, mode="overwrite")
    # 2
    df2=spark.sql("SELECT Date FROM table1 ORDER BY High DESC LIMIT 1")
    df2.write.csv("/root/airflow/outputfiles/df2.csv",header=True,mode="overwrite")
    # 3
    df3=spark.sql("SELECT MAX(Volume) AS max_volume, MIN(Volume) AS min_volume FROM table1")
    df3.write.csv("/root/airflow/outputfiles/df3.csv",header=True,mode="overwrite")
    #4
    df4=spark.sql("SELECT COUNT(*) AS num_days FROM table1 WHERE Close < 60")
    df4.write.csv("/root/airflow/outputfiles/df4.csv",header=True,mode="overwrite")
    #5
    df5=spark.sql("SELECT (COUNT(CASE WHEN High > 80 THEN 1 END) / COUNT(*)) * 100 AS high_percentage FROM table1")
    #df5.write.format("csv").mode("overwrite").save("/root/airflow/outputfiles/df5.csv")
    df5.write.csv("/root/airflow/outputfiles/df5.csv",header=True,mode="overwrite")
    #6
    df6=spark.sql("select corr(High, Volume) from table1")
    #df6.write.format("csv").mode("overwrite").save("/root/airflow/outputfiles/df6.csv")
    df6.write.csv("/root/airflow/outputfiles/df6.csv",header=True,mode="overwrite")
    #7
    df7=spark.sql("select YEAR(Date) as year,MAX(High) as MaxHigh From table1 GROUP BY  YEAR(Date) ORDER BY Year")
    df7.write.csv("/root/airflow/outputfiles/df7.csv",header=True,mode="overwrite")
    #8
    df8=spark.sql("select MONTH(Date) as Month,AVG(Close) as AvgClose From table1 GROUP BY  MONTH(Date) ORDER BY Month")
    df8.write.csv("/root/airflow/outputfiles/df8.csv",header=True,mode="overwrite")



send_email = EmailOperator(
        task_id='send_email',
        to='anilkumarjat06@gmail.com',
        subject='ingestion complete',
        html_content="task done vrify email",
        dag=dags)
start_task=DummyOperator(task_id='start_task',dag=dags)
end_task=DummyOperator(task_id='end_task',dag=dags)
spark_task=PythonOperator(task_id='spark_task',python_callable=process_data,dag=dags)
start_task>>spark_task>>end_task>>send_email




