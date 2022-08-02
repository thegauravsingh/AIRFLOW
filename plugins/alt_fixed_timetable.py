from datetime import timedelta
from typing import Optional
from pendulum import Date, DateTime, Time, timezone, parse
import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from sears_business_calendar import SearsBusinessCalendar
from airflow.plugins_manager import AirflowPlugin
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable

UTC = timezone("UTC")

holiday_cal = SearsBusinessCalendar()
holiday_df = pd.DataFrame()
holiday_df['Date'] = holiday_cal.holidays(start='2022-01-01', end='2022-12-31').to_frame(index=False)

mondays = pd.date_range(start='2022-01-01', end='2022-12-31', freq='W-MON')
tuesdays = pd.date_range(start='2022-01-01', end='2022-12-31', freq='W-TUE')
df = pd.DataFrame()
df['Date'] = mondays.union(tuesdays)
bd = pd.tseries.offsets.BusinessDay(n = 1)
holidays_in_df = pd.merge(df, holiday_df, how = 'inner', on = 'Date') 
holidays_in_df_advanced_by_working_day = pd.DataFrame()
holidays_in_df_advanced_by_working_day['Date'] = holidays_in_df['Date']  + bd
df_without_holidays = df[df.Date.isin(holiday_df.Date) == False]
fixed_calendar_df = pd.concat([df, holidays_in_df_advanced_by_working_day], axis=0, ignore_index=True)
#print('fixed_calendar_df :',fixed_calendar_df)
class AltFixedTimetable(Timetable):

    def infer_manual_data_interval(self, run_after: DateTime) -> DataInterval:
        run_after_str = run_after.format('YYYY-MM-DD')
        rows = (fixed_calendar_df['Date'] >= run_after_str) 
        remaining_holidays = fixed_calendar_df.loc[rows]
        start = parse(str(remaining_holidays['Date'].iloc[0])).naive()
        start = start.replace(tzinfo=UTC)
        end = start + timedelta(days=1)
        end =  end.replace(tzinfo=UTC)
        print("manual start: ", start.format('YYYY-MM-DD'))        
        print("manual end: ", end.format('YYYY-MM-DD'))      
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
            remaining_holidays = fixed_calendar_df.loc[rows]
            next_start = parse(str(remaining_holidays['Date'].iloc[0])).naive()
            next_end = next_start + timedelta(days=1)
            print('previous run on the regular schedule next_start :', next_start)
            print('previous run on the regular schedule next_end :', next_end)            
            print('previous run on the regular schedule remaining_holidays :', remaining_holidays)                  
            next_start = next_start.replace(tzinfo=UTC)
            next_end = next_end.replace(tzinfo=UTC)       
      
        else:  # This is the first ever run on the regular schedule.
            next_start = restriction.earliest
            if next_start is None:  # No start_date. Don't schedule.
                return None
            if not restriction.catchup: # If the DAG has catchup=False, today is the earliest to consider.
                next_start = max(next_start, DateTime.combine(Date.today(), Time.min).replace(tzinfo=UTC))
            next_start_str = next_start.format('YYYY-MM-DD')
            rows = (fixed_calendar_df['Date'] >= next_start_str)             
            remaining_holidays = fixed_calendar_df.loc[rows]
            print('first ever run on the regular schedule next_start_str :', next_start_str)
            print('first ever run on the regular schedule remaining_holidays :', remaining_holidays)
            
            if remaining_holidays is not None:
                next_start = parse(str(remaining_holidays['Date'].iloc[0])).naive()
                next_end = next_start + timedelta(days=1)
                print('next_start :',next_start)
                print('next_end :',next_end)    
                next_start = next_start.replace(tzinfo=UTC)
                next_end = next_end.replace(tzinfo=UTC)
            else:
                next_end = next_start.subtract(days=1).replace(tzinfo=UTC)  
        if restriction.latest is not None and next_start > restriction.latest:
            return None  # Over the DAG's scheduled end; don't schedule.
        return DagRunInfo.interval(start=next_start, end=next_end)

class FixedTimetablePlugin(AirflowPlugin):
    name = "alt_fixed_timetable_plugin"
    timetables = [AltFixedTimetable]
