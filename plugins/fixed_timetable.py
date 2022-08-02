from datetime import timedelta
from typing import Optional
from pendulum import Date, DateTime, Time, timezone, parse
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from sears_business_calendar import SearsBusinessCalendar
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")

#Gotham_BD = CustomBusinessDay(calendar=SearsBusinessCalendar())
#s = pd.date_range('2018-07-01', end='2018-07-31', freq=Gotham_BD)
#df = pd.DataFrame(s, columns=['Date'])
#holiday_cal = SearsBusinessCalendar()
#df1 = df2 = pd.DataFrame()
#df1['Date'] = holiday_cal.holidays(start='2022-01-01', end='2022-12-31').to_frame(index=False)
#
#class FixedTimetable(Timetable):
#    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
#        pass

holiday_cal = SearsBusinessCalendar()
holiday_df = pd.DataFrame()
holiday_df['Date'] = holiday_cal.holidays(start='2022-01-01', end='2022-12-31').to_frame(index=False)
fixed_calendar_df = holiday_df
class FixedTimetable(Timetable):

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        run_after_str = run_after.format('YYYY-MM-DD')
        rows = (fixed_calendar_df['Date'] >= run_after_str) 
        remaining_calendar_days = fixed_calendar_df.loc[rows]
        start = parse(str(remaining_calendar_days['Date'].iloc[0])).naive()
        start = start.replace(tzinfo=UTC)
        end =   start + timedelta(days=1)     
        return DataInterval(start=start, end=end)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: Optional[DataInterval],
        restriction: TimeRestriction,
    ) -> Optional[DagRunInfo]:
        if last_automated_data_interval is not None:  # There was a previous run on the regular schedule.
            last_start = last_automated_data_interval.start
            last_start_str = last_start.format('YYYY-MM-DD')
            rows = (fixed_calendar_df['Date'] > last_start_str) 
            remaining_calendar_days = fixed_calendar_df.loc[rows]
            next_start = parse(str(remaining_calendar_days['Date'].iloc[0])).naive()
            next_start = next_start.replace(tzinfo=UTC)
            next_end = next_start + timedelta(days=1)
            print('previous run on the regular schedule next_start :', next_start)
            print('previous run on the regular schedule next_end :', next_end)            
            print('previous run on the regular schedule remaining_calendar_days :', remaining_calendar_days)                           
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            next_start_str = next_start.format('YYYY-MM-DD')
            rows = (fixed_calendar_df['Date'] >= next_start_str)             
            remaining_calendar_days = fixed_calendar_df.loc[rows]
            print('first ever run on the regular schedule next_start_str :', next_start_str)
            print('first ever run on the regular schedule remaining_calendar_days :', remaining_calendar_days)
            if remaining_calendar_days is not None:
                next_start = parse(str(remaining_calendar_days['Date'].iloc[0])).naive()
                next_start = next_start.replace(tzinfo=UTC)
                next_end = next_start + timedelta(days=1)
            else:
                next_end = next_start.subtract(days=1).replace(tzinfo=UTC)  
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)



class FixedTimetablePlugin(AirflowPlugin):
    name = "fixed_timetable_plugin"
    timetables = [FixedTimetable]
