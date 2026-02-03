def timestamp_offset(current_timestamp, offset_hours):
    """
    Calculates the timestamp used by the api, offset by number of specified hours
    args: current_timestamp-date: string format'2026-01-30T14:00', offset_ours: int number of hours for datetime to be offset.
    returns: offset timestamp in string format'2026-01-30T14:00'
    """
    import datetime as dt
    year = int(current_timestamp[:4])
    month = int(current_timestamp[5:7])
    day = int(current_timestamp[8:10])
    hour = int(current_timestamp[11:13])
    minute = int(current_timestamp[14:16])
    second = 0
    current_time =  dt.datetime(year, month, day, hour, minute, second)
    offset_time = current_time + dt.timedelta(hours=offset_hours)
    return f'{offset_time.year:>04}-{offset_time.month:>02}-{offset_time.day:>02}T{offset_time.hour:>02}:{offset_time.minute:>02}'
