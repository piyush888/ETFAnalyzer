from pandas.tseries.holiday import USFederalHolidayCalendar
import datetime
from pandas.tseries.offsets import BDay


def HolidayCheck(dateToCheck):
	cal=USFederalHolidayCalendar()
	currentYear = dateToCheck.year
	startDate = str(currentYear)+'-01-01'
	endDate = str(currentYear+1)+'-01-01'
	
	# holidays=cal.holidays(start='2020-01-01', end='2021-01-01').to_pydatetime()
	Federalholidays=cal.holidays(start=startDate, end=endDate).to_pydatetime()

	if dateToCheck in Federalholidays:
		return True
	else:
		weekno = datetime.datetime.today().weekday()
		print(weekno)
		return True if weekno>=5 else False
	
def LastWorkingDay(dateToCheck):
	return dateToCheck - BDay(1)

def isTimeBetween(begin_time=None, end_time=None, check_time=None):
	# If check time is not given, default to current UTC time
	check_time = check_time or datetime.estnow().time()
	if not end_time:
		return check_time >= begin_time
	else:
		if begin_time < end_time:
			return check_time >= begin_time and check_time <= end_time
		else: # crosses midnight
			return check_time >= begin_time or check_time <= end_time