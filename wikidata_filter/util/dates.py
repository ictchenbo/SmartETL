import re
import time
from datetime import datetime


ONE_DAY = 86400
MONTH_DAYS = [
    [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
]
YEAR_DAYS = [365, 366]


def date2ts(dt: str, fmt='%Y-%m-%d', millis=False) -> int:
    """字符串日期转时间戳"""
    time1 = datetime.strptime(dt, fmt)
    time2 = datetime.strptime("1970-01-01 00:00:00", fmt)

    diff = time1 - time2
    if millis:
        return diff.days * 24 * 3600000 + diff.seconds*1000 + diff.microseconds
    return diff.days * 24 * 3600 + diff.seconds  # 换算成秒数


def ts2datetime(ts, fmt='%Y-%m-%d %H:%M:%S') -> str:
    """时间戳转字符串日期"""
    if ts is not None:
        return time.strftime(fmt, time.localtime(ts))
    return ""


def current_ts(millis=False) -> int:
    """当前时间时间戳"""
    if millis:
        return int(time.time()*1000)
    return int(time.time())


def current_date(fmt='%Y-%m-%d') -> str:
    """当前日期字符串"""
    return ts2datetime(current_ts(), fmt=fmt)


def current_time(fmt='%Y-%m-%d %H:%M:%S') -> str:
    """当前时间字符串"""
    return ts2datetime(current_ts(), fmt=fmt)


def obj2ts(d: datetime, fmt='%Y-%m-%d %H:%M:%S', millis=False):
    """日期对象转时间戳"""
    dt_str = d.strftime(fmt)
    return date2ts(dt_str, fmt, millis)


def is_leap_year(year: int):
    if year % 400 == 0 or (year % 4 == 0 and year % 100 != 0):
        return 1
    return 0


def month_days(year: int, month: int):
    return MONTH_DAYS[is_leap_year(year)][month]


def expand_date_range(dt: str):
    parts = re.split('[年月日\\-]+', dt, maxsplit=3)
    parts = [p for p in parts if p]
    # print(parts)
    if len(parts) == 3:
        ts = date2ts('-'.join(parts))
        return ts, ts + ONE_DAY
    if len(parts) == 2:
        ts = date2ts(f'{parts[0]}-{parts[1]}-01')
        days = month_days(int(parts[0]), int(parts[1]))
        return ts, ts + ONE_DAY * days
    if len(parts) == 1:
        year = parts[0]
        ts = date2ts(f'{year}-01-01')
        return ts, ts + ONE_DAY * YEAR_DAYS[is_leap_year(int(year))]
    raise Exception("Invalid date")


def fill_date(dt: str or list):
    if isinstance(dt, str):
        return expand_date_range(dt)
    else:
        if len(dt) == 1:
            return expand_date_range(dt[0])
        else:
            return expand_date_range(dt[0])[0], expand_date_range(dt[1])[1]


def normalize_time(t: str):
    return t
