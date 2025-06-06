from datetime import datetime, timedelta
from typing import Literal
from dateutil.relativedelta import relativedelta

class DateAndTime:
    def __init__(self):
        pass

    @classmethod
    def _generate_date_list_desc(cls, start_date, end_date=None, interval=1, interval_type: Literal['days', 'weeks', 'months', 'years']='days'):
        date_list = []
        if end_date is None:
            end_date = datetime.today()
        current = end_date
        while current >= start_date:
            date_list.append(current.strftime("%Y-%m-%d"))
            if interval_type in ["months", "years"]:
                kwargs = {interval_type: interval}
                current -= relativedelta(**kwargs)
            else:
                kwargs = {interval_type: interval}
                current -= timedelta(**kwargs)
        return date_list

    @classmethod
    def _generate_date_list_asc(cls, start_date, end_date=None, interval=1, interval_type: Literal['days', 'weeks', 'months', 'years']='days'):
        date_list = []
        if end_date is None:
            end_date = datetime.today()
        current = start_date
        while current <= end_date:
            date_list.append(current)
            if interval_type in ["months", "years"]:
                kwargs = {interval_type: interval}
                current += relativedelta(**kwargs)
            else:
                kwargs = {interval_type: interval}
                current += timedelta(**kwargs)
        return date_list

    @classmethod
    def generate_date_list(cls, start_date, end_date=None, ascending=True, interval=1, interval_type: Literal['days', 'weeks', 'months', 'years']='days'):
        # Accept both string and datetime input
        if isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
            except Exception as e:
                raise ValueError(f"start_date must be in 'YYYY-MM-DD' format or a datetime object: {e}")
        if end_date is not None and isinstance(end_date, str):
            try:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
            except Exception as e:
                raise ValueError(f"end_date must be in 'YYYY-MM-DD' format or a datetime object: {e}")
        if end_date is None:
            if interval_type in ["months", "years"]:
                end_date = start_date + relativedelta(**{interval_type: interval})
            else:
                end_date = start_date + timedelta(**{interval_type: interval})

        if ascending:
            date_list = cls._generate_date_list_asc(start_date=start_date, end_date=end_date, interval=interval, interval_type=interval_type)
        else:
            date_list = cls._generate_date_list_desc(start_date=start_date, end_date=end_date, interval=interval, interval_type=interval_type)
        return date_list


