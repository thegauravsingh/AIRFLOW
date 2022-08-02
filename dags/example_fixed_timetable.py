from datetime import timedelta
from typing import Dict

from airflow.decorators import task
from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weekday import WeekDay
from airflow.utils.dates import days_ago

from airflow import plugins_manager
plugins_manager.initialize_timetables_plugins()
from fixed_timetable import FixedTimetable


"""
Example DAG to show use of custom timetable.
"""

# Reference data for determining the activity to perform per day of week.
DAY_ACTIVITY_MAPPING = {
    "monday": {"is_weekday": True, "activity": "guitar lessons"},
    "tuesday": {"is_weekday": True, "activity": "studying"},
    "wednesday": {"is_weekday": True, "activity": "soccer practice"},
    "thursday": {"is_weekday": True, "activity": "contributing to Airflow"},
    "friday": {"is_weekday": True, "activity": "family dinner"},
    "saturday": {"is_weekday": False, "activity": "going to the beach"},
    "sunday": {"is_weekday": False, "activity": "sleeping in"},
}


@task(multiple_outputs=True)
def going_to_the_beach() -> Dict:
    return {
        "subject": "Beach day!",
        "body": "It's Saturday and I'm heading to the beach.<br><br>Come join me!<br>",
    }


def _get_activity(day_name) -> str:
    activity_id = DAY_ACTIVITY_MAPPING[day_name]["activity"].replace(" ", "_")

    if DAY_ACTIVITY_MAPPING[day_name]["is_weekday"]:
        return f"weekday_activities.{activity_id}"

    return f"weekend_activities.{activity_id}"


with DAG(
    dag_id="example-fixed-timetable",
    start_date=days_ago( 180 ), 
    #max_active_runs=1,
    timetable=FixedTimetable(),
    tags=["example", "timetable", "hr-it-dev"],
    #default_args={
    #    "retries": 1,
    #    "retry_delay": timedelta(minutes=3),
    #},
    catchup=True
) as dag:
    DummyOperator(task_id="run_this")