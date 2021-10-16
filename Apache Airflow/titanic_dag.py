import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.hive_operator import HiveOperator

args = {
   'owner': 'airflow',
   'start_date': dt.datetime(2020, 12, 23),  
   'retries': 1, 
   'retry_delay': dt.timedelta(minutes=1), 
   'depends_on_past': False,  
}

def get_path(file_name):
   return os.path.join(os.path.expanduser('~'), file_name)


def download_titanic_dataset():
   url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
   df = pd.read_csv(url)
   df.to_csv(get_path('titanic.csv'), encoding='utf-8')

def pivot_dataset():
   titanic_df = pd.read_csv(get_path('titanic.csv'))
   df = titanic_df.pivot_table(index=['Pclass'],
                               values=['Fare'],
                               aggfunc='mean').reset_index()
   df.to_csv(get_path('titanic_pivot.csv'), index=False)


with DAG(
       dag_id='titanic_pivot',  
       schedule_interval=None,  
       default_args=args,  
) as dag:

	first_task = BashOperator(
   	task_id='first_task',
  	 bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
  	 dag=dag,
	)

	create_titanic_dataset = PythonOperator(	
   	task_id='download_titanic_dataset',
   	python_callable=download_titanic_dataset,
   	dag=dag,
	)

	pivot_titanic_dataset = PythonOperator(
  	 task_id='pivot_dataset',
  	 python_callable=pivot_dataset,
  	 dag=dag,
	)

	hdfs_operations_task = BashOperator(
    		task_id='hdfs_operations_task',
    		bash_command='hdfs dfs -copyFromLocal titanic_pivot.csv',
    		dag=dag
	)
	
	hive_table= HiveOperator(
    		hql="CREATE SCHEMA IF NOT EXISTS titanic;"+\
		     "CREATE EXTERNAL TABLE IF NOT EXISTS bdp.hv_csv_table "+\
		     "(Pclass INT Fare DECIMAL)"+\
		     "ROW FORMAT DELIMITED"+\
		     "FIELDS TERMINATED BY ','"+\
		     "STORED AS TEXTFILE"+\
		     "LOCATION 'titanic_pivot.csv';",
    		hive_cli_conn_id='hive_staging',
   		schema='default',
   		hiveconf_jinja_translate=True,
   		task_id='hive_table',
   		dag=dag)




	check_titanic = BashOperator(
	 task_id='check',
	 bash_command='echo "Pipeline finished! Execution date is {(date)}."',
	 dag=dag,
	)


first_task >> create_titanic_dataset >> pivot_titanic_dataset >> hdfs_operations_task >> hive_table >> check_titanic
