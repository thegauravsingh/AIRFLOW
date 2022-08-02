from airflow.utils.dates import days_ago
from airflow import DAG
from airflow import plugins_manager
from airflow.operators.dummy import DummyOperator

plugins_manager.initialize_timetables_plugins()
from weekday_timetable import AfterWorkdayTimetable

with DAG(
    dag_id="example-workday-timetable",
    start_date=days_ago( 30 ),
    timetable=AfterWorkdayTimetable(),
    tags=["example", "timetable", "hr-it-dev"],
) as dag:
    DummyOperator(task_id="run_this")