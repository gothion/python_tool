# coding=utf-8

from datetime import date, timedelta
import time


def get_yesterday(time_format='%Y%m%d'):
    return (date.today() - timedelta(1)).strftime(time_format)


def get_current(time_format='%Y%m%d'):
    local_time = time.localtime()
    time_string = time.strftime(time_format, local_time)
    return time_string
    # return date.today().strftime(time_format)


def get_date(diff_days=0, diff_hours=0, day_format='%Y%m%d', current_date=None):
    date = {}
    if current_date is None:
        timestamp = time.time() - (diff_days * 24 + diff_hours) * 3600
        date['day'], date['hour'] = time.strftime(day_format + ' %H:%M', time.localtime(timestamp)).split()
        return date

    day = current_date.get('day', None)
    hour = current_date.get('hour', None)
    if hour is None:
        timestamp = time.mktime(time.strptime(day, day_format)) - diff_days * 24 * 3600
        date['day'] = time.strftime(day_format, time.localtime(timestamp))
        return date
    else:
        timestamp = time.mktime(time.strptime('%s %s' % (day, hour), day_format + ' %H:%M')) - \
            (diff_days * 24 + diff_hours) * 3600
        date['day'], date['hour'] = time.strftime(day_format + ' %H:%M', time.localtime(timestamp)).split()
        return date


def get_today(time_format='%Y%m%d'):
    return(date.today()).strftime(time_format)


def get_last_half_hour(time_format='%H-%M-%S'):
    localtime = time.localtime()
    minute = localtime.tm_min

    if minute > 30:
        minute = 30
    else:
        minute = 0

    last_hour_time = str(localtime.tm_year) + convert_digit_to_str(localtime.tm_mon) + convert_digit_to_str(localtime.tm_mday) + ' '\
        + convert_digit_to_str(localtime.tm_hour) + '-' + convert_digit_to_str(minute) + '-' + '00'
    timestamp = time.mktime(time.strptime(last_hour_time, '%Y%m%d %H-%M-%S'))

    return time.strftime(time_format, time.localtime(timestamp))


def convert_digit_to_str(digit):
    if digit < 10:
        return str(0) + str(digit)
    return str(digit)


def get_before_last_half_hour(time_format='%H-%M-%S'):
    localtime = time.localtime()
    minute = localtime.tm_min
    if minute > 30:
        minute = 30
    else:
        minute = 0

    last_hour_time = str(localtime.tm_year) + convert_digit_to_str(localtime.tm_mon) + convert_digit_to_str(localtime.tm_mday) + ' '\
        + convert_digit_to_str(localtime.tm_hour) + '-' + convert_digit_to_str(minute) + '-' + '00'
    timestamp = time.mktime(time.strptime(last_hour_time, '%Y%m%d %H-%M-%S')) - (30 * 60)
    return time.strftime(time_format, time.localtime(timestamp))


def get_week_before(day_time, time_format='%Y%m%d'):
    timestamp = time.mktime(time.strptime(day_time, time_format))
    timestamp -= 7*24*3600
    return time.strftime(time_format, time.localtime(timestamp))


def get_day_before(day_time, num_day, time_format='%Y%m%d'):
    timestamp = time.mktime(time.strptime(day_time, time_format))
    timestamp -= num_day*24*3600
    return time.strftime(time_format, time.localtime(timestamp))


def get_minute_before(time_str, minute, time_format='%H-%M'):
    timestamp = time.mktime(time.strptime(time_str, time_format))
    timestamp -= minute * 60
    return time.strftime(time_format, time.localtime(timestamp))


def convert_time(time_str, input_format, out_format):
    timestamp = time.mktime(time.strptime(time_str, input_format))
    return time.strftime(out_format, time.localtime(timestamp))


def get_time_seconds(before_days=1):
    time_seconds = time.time() - before_days * 24 * 3600
    return int(time_seconds)


def get_yesterday_timestamp():
    return int(time.mktime((date.today() - timedelta(1)).timetuple()))

if __name__ == '__main__':
    print get_yesterday()
    print get_current('%Y%m%d_%H%M%S')
    print get_last_half_hour()
    print get_before_last_half_hour()
    print get_week_before(str(20151224))
    print get_current(time_format='%Y-%m-%d %H:%M:%S')
    print get_last_half_hour(time_format='%Y%m%d')
    # print convert_time("05:43", "%H:%M", "%H_%M")
    daytime = get_yesterday('%Y%m')
    print daytime
    print get_day_before(daytime, 7, '%Y%m')
