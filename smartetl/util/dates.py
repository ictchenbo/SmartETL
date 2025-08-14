import re
import time
import pytz
from datetime import datetime, timezone, timedelta


ONE_DAY = 86400
MONTH_DAYS = [
    [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
    [0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
]
YEAR_DAYS = [365, 366]


def date2ts(dt: str, fmt='%Y-%m-%d', millis=False) -> int:
    """字符串日期转时间戳"""
    time1 = datetime.strptime(dt, fmt)
    time2 = datetime.strptime("1970-01-01", fmt)

    diff = time1 - time2
    if millis:
        return diff.days * 24 * 3600000 + diff.seconds*1000 + diff.microseconds
    return diff.days * 24 * 3600 + diff.seconds  # 换算成秒数


def ts2datetime(ts, fmt='%Y-%m-%d %H:%M:%S') -> str:
    """时间戳转字符串日期"""
    if ts is not None:
        return time.strftime(fmt, time.localtime(ts))
    return ""


def millis2datetime(ts, fmt='%Y-%m-%d %H:%M:%S') -> str:
    """毫秒时间戳转字符串日期"""
    return ts2datetime(ts/1000, fmt=fmt)


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


def normalize_isotime(ios_datetime_str: str):
    """
        基于IOSdate格式日期/时间转化为北京时间时间戳
    """
    dt = datetime.fromisoformat(ios_datetime_str.replace("Z", "+00:00"))  # 替换 Z 为 UTC 时区
    beijing_dt = dt.astimezone(timezone(timedelta(hours=8)))
    return int(beijing_dt.timestamp() * 1000)


possible_formats = [
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d-%m-%Y %H:%M:%S",
    "%m/%d/%y %H:%M:%S",  # 美国
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m/%d/%y"  # 美国
]


# beijing_tz = pytz.timezone('Asia/Shanghai')
beijing_tz = timezone(timedelta(hours=8))


def custom_datetime_to_beijing_timestamp(datetime_str, format_str, tz=None):
    """基于指定格式进行解析并转化为北京时间 假设为UTC"""
    dt = datetime.strptime(datetime_str, format_str)
    dt = dt.replace(tzinfo=tz or timezone.utc)
    beijing_dt = dt.astimezone(beijing_tz)
    return int(beijing_dt.timestamp() * 1000)


TIMEZONE_ABBREVIATIONS = {
    "IST": "Asia/Kolkata",      # 印度标准时间 UTC+5:30
    "PST": "America/Los_Angeles", # 太平洋标准时间 UTC-8
    "EST": "America/New_York",   # 东部标准时间 UTC-5
    "CST": "America/Chicago",    # 中部标准时间 UTC-6
    "MST": "America/Denver",     # 山地标准时间 UTC-7
    "JST": "Asia/Tokyo",         # 日本标准时间 UTC+9
    "CET": "Europe/Berlin",      # 中欧时间 UTC+1
    "EET": "Europe/Istanbul",    # 东欧时间 UTC+2
    "BST": "Europe/London",      # 英国夏令时 UTC+1
    "AEDT": "Australia/Sydney",  # 澳大利亚东部夏令时 UTC+11
    "ACST": "Australia/Adelaide",# 澳大利亚中部标准时间 UTC+9:30
}


def native_datetime_to_beijing_timestamp(time_str: str, time_format: str = '%B %d, %Y %I:%M:%S %p %Z', tz_name: str = None):
    """
    将任意时区的时间字符串转换为北京时间时间戳。

    参数：
    - time_str (str): 输入的时间字符串，例如 "March 3, 2025 4:13:13 PM IST"。
    - time_format (str): 时间字符串的格式，例如 "%B %d, %Y %I:%M:%S %p %Z"。
    - tz_name (str): 输入时间字符串所属的时区名称，例如 "Asia/Kolkata"。

    返回：
    - int: 北京时间的时间戳（秒）。
    """
    try:
        if tz_name is None:
            abbr = time_str.split()[-1]
            if abbr not in TIMEZONE_ABBREVIATIONS:
                return None
            tz_name = TIMEZONE_ABBREVIATIONS[abbr]

        input_tz = pytz.timezone(tz_name)

        # 将时间字符串解析为 naive datetime 对象（无时区）
        naive_dt = datetime.strptime(time_str, time_format)

        # 赋予输入时区信息
        localized_dt = input_tz.localize(naive_dt)

        # 转换为北京时间
        beijing_dt = localized_dt.astimezone(beijing_tz)

        return int(beijing_dt.timestamp()*1000)

    except Exception as e:
        raise ValueError(f"转换失败：{e}")


def normalize_time(datetime_str: str, tz=None):
    """解析时间,转化为北京时间时间戳"""
    # 先根据ISO和非ISO的连接符判断
    if '-' not in datetime_str and '/' not in datetime_str:
        print('无法解析时间：', datetime_str)
        return None

    # 假设为ISO格式
    try:
        iso_date = normalize_isotime(datetime_str)
        return iso_date
    except ValueError as e:
        print("not ISO format:", datetime_str)

    # 尝试按照几个格式进行解析
    for fmt in possible_formats:
        try:
            timestamp_ms = custom_datetime_to_beijing_timestamp(datetime_str, fmt)
            return timestamp_ms
        except Exception as e:
            pass

    print("无法解析时间：", datetime_str)
    return None


if __name__ == "__main__":
    # 示例用法
    print(normalize_isotime("1925-01-09T14:23:45.056+03:00"))
    print(normalize_isotime("1971-01-01"))
    print(normalize_isotime("1971-01-01T00:02:34-05:00"))
    # 非ISO格式
    print(normalize_isotime("1971-01-02 15:01:08"))
