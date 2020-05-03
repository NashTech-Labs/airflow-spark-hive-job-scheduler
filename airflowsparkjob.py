import airflow
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator

DAG_NAME = 'Demo'
args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(1),
}

dag_prjt_main = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    schedule_interval='* * * * *' #"@once"
)

SQOOP_Task1 = BashOperator(task_id="Sqoop",
                      bash_command='~/sqoop-1.4.7.bin__hadoop-2.6.0/bin/sqoop job --exec Sqoop_weblogdetails_test37', dag=dag_prjt_main)

hive_cmd= """use test1;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=1000;
insert into weblog_partiton_table partition(host) select id, datevalue, ipaddress, url, responsecode, host from weblog_external as a where not exists(select b.id from weblog_partiton_table as b where a.id = b.id);"""

hive_part = HiveOperator(hive_cli_conn_id='hive_cli_default', hql=hive_cmd, task_id = 'Hive', dag=dag_prjt_main)

finish_task = DummyOperator(task_id="finaltask", dag=dag_prjt_main)

SQOOP_Task1 >> hive_part >> finish_task


if __name__ == '__main__':
    dag_prjt_main.cli()

