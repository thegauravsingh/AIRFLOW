from airflow.utils.dates import days_ago
from airflow import DAG
from airflow import plugins_manager
from airflow.operators.dummy import DummyOperator

plugins_manager.initialize_timetables_plugins()
from end_of_month_timetable import EndOfMonthTimetable

with DAG(
    dag_id="example-end-of-month-timetable",
    start_date=days_ago( 90 ),
    timetable=EndOfMonthTimetable(),
    tags=["example", "timetable", "hr-it-dev"],
) as dag:
    DummyOperator(task_id="run_this")