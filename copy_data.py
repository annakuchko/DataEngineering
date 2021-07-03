from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import BashOperator, PythonOperator
import psycopg2
import csv
import string
import os


SRC = "host='172.17.0.1' port='5434' dbname='my_database' user='root' password='postgres'" 
TRG = "host='172.17.0.1' port='5433' dbname='my_database' user='root' password='postgres'"
    
def get_tables(cursor):
    cursor.execute("""SELECT table_name FROM information_schema.tables 
                       WHERE table_schema = 'public'""")
    tables = [tbl[0] for tbl in cursor.fetchall()]
    return tables

def check_tables(source_conn_string=SRC, 
                 target_conn_string=TRG, 
                 verbose=False):
    with psycopg2.connect(source_conn_string) as \
        conn_source, conn_source.cursor() as cursor_source:
        source_tables = get_tables(cursor_source)
        with psycopg2.connect(target_conn_string) as \
            conn_target, conn_target.cursor() as cursor_target:
            target_tables = get_tables(cursor_target)
    
            for tbl in source_tables:
                if tbl not in target_tables:
                    if verbose:
                        print(f"Creating new table {tbl}")
                    src_schema = cursor_source.execute(
                        f"SELECT column_name, data_type, "
                        f"character_maximum_length  FROM "
                        f"information_schema.columns WHERE "
                        f"table_name = '{tbl}';")
                    sch = [item for t in [
                        i for i in cursor_source
                        ] for item in t]
                    sch = ','.join(
                        [
                            ' '.join(j) for j in [
                                [
                                    str(sch[i]), str(sch[i+1])+'('+str(sch[i+2])+')'
                                    ] for i in range(0,len(sch)-2, 3)
                                ]
                            ]
                        ).replace('()', '').replace('(None)', '')
                    targ_schema = cursor_target.execute(
                        f"CREATE TABLE {tbl} ({sch});"
                        )
        conn_target.commit()
        
def trim_spaces(file):
    with open(f"{file}.csv") as f:
        reader = csv.reader(f, delimiter=',')
        with open(f"{file}_1.csv", "w", newline='\n') as fo:
            writer = csv.writer(fo)
            for row in reader:
                writer.writerow([r.strip() for r in row])
    os.remove(f'{file}.csv')
                
def copy_data(source_conn_string=SRC, target_conn_string=TRG, verbose=False):
    with psycopg2.connect(source_conn_string) as \
        conn_source, conn_source.cursor() as cursor_source:
        source_tables = get_tables(cursor_source)
    for tbl in source_tables:
        with psycopg2.connect(source_conn_string) as \
            conn_source, conn_source.cursor() as cursor_source:
            q = f"COPY {tbl} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            with open(f'{tbl}.csv', 'w') as f:
                cursor_source.copy_expert(q, f)
            trim_spaces(tbl)
        with psycopg2.connect(target_conn_string) as \
            conn_target, conn_target.cursor() as cursor_target:   
            q = f"COPY {tbl} from STDIN WITH DELIMITER ',' CSV HEADER;"
            with open(f'{tbl}_1.csv', 'r') as f:
                cursor_target.copy_expert(q, f)            
            os.remove(f'{tbl}_1.csv')
        conn_target.commit()



DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2021, 6, 19),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}

with DAG(
    dag_id="copy_data",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    tags=['data-flow'],
) as dag:
	t1 = PythonOperator(
		task_id='check_tbl_exists',
		provide_context=False,
    		python_callable=check_tables
	)


	t2 = PythonOperator(
		task_id='copy_data_to_target',
		provide_context=False,
    		python_callable=copy_data)

	t1 >> t2
