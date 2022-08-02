from pandas.tseries.holiday import *

class SearsBusinessCalendar(AbstractHolidayCalendar):
   rules = [
     Holiday('New Year', month=1, day=1, observance=sunday_to_monday),
     Holiday('Groundhog Day', month=1, day=6, observance=sunday_to_monday),
     Holiday('St. Patricks Day', month=3, day=17, observance=sunday_to_monday),
     Holiday('tst. Patricks Day', month=3, day=18, observance=sunday_to_monday),     
     Holiday('April Fools Day', month=4, day=1),
     #Holiday('Good Friday', month=1, day=1, offset=[Easter(), Day(-2)]),
     USMemorialDay,
     Holiday('Labor Day', month=5, day=1, observance=sunday_to_monday),
     Holiday('Canada Day', month=7, day=1, observance=sunday_to_monday),
     Holiday('July 4th', month=7, day=4, observance=nearest_workday),
     USLaborDay,
     USThanksgivingDay,
     #Holiday('All Saints Day', month=11, day=1, observance=sunday_to_monday),
     Holiday('Christmas', month=12, day=25, observance=nearest_workday)
   ]