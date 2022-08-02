import pandas as pd
from pandas.tseries.offsets import CustomBusinessDay
from sears_business_calendar import SearsBusinessCalendar


holiday_cal = SearsBusinessCalendar()
holiday_df = pd.DataFrame()
holiday_df['Date'] = holiday_cal.holidays(start='2022-01-01', end='2022-12-31').to_frame(index=False)
print(holiday_df)
mondays = pd.date_range(start='2022-01-01', end='2022-12-31', freq='W-MON')
tuesdays = pd.date_range(start='2022-01-01', end='2022-12-31', freq='W-TUE')
df = pd.DataFrame()
df['Date'] = mondays.union(tuesdays)
bd = pd.tseries.offsets.BusinessDay(n = 1)
holidays_in_df = pd.merge(df, holiday_df, how = 'inner', on = 'Date') 
#holidays_in_df = df_with_holidays[df_with_holidays.Date.isin(holiday_df.Date) == True]
holidays_in_df_advanced_by_working_day = pd.DataFrame()
holidays_in_df_advanced_by_working_day['Date'] = holidays_in_df['Date']  + bd
df_without_holidays = df[df.Date.isin(holiday_df.Date) == False]
print(holidays_in_df_advanced_by_working_day.Date.duplicated().sum())
main_df_with_holidays_advanced_by_working_day = pd.concat([df_without_holidays, holidays_in_df_advanced_by_working_day], axis=0, ignore_index=True)
 
print(main_df_with_holidays_advanced_by_working_day.Date.duplicated().sum())

print(main_df_with_holidays_advanced_by_working_day)