"""ISO 8601 date time string parsing

Basic usage:
>>> import iso8601
>>> iso8601.parse_date("2007-01-25T12:00:00Z")
datetime.datetime(2007, 1, 25, 12, 0, tzinfo=<iso8601.iso8601.Utc ...>)
>>>

http://code.google.com/p/pyiso8601/

License from package:
Copyright (c) 2007 Michael Twomey


Permission is hereby granted, free of charge, to any person obtaining a
copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:


The above copyright notice and this permission notice shall be included
in all copies or substantial portions of the Software.


THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

from datetime import datetime, timedelta, tzinfo
import re

__all__ = ["parse_date", "print_date", "ParseError"]

# Adapted from http://delete.me.uk/2005/03/iso8601.html
ISO8601_REGEX = re.compile(r"(?P<year>[0-9]{4})(-?(?P<month>[0-9]{1,2})(-?(?P<day>[0-9]{1,2})"
    r"((?P<separator>.)(?P<hour>[0-9]{2}):?(?P<minute>[0-9]{2})(:?(?P<second>[0-9]{2})(\.(?P<fraction>[0-9]+))?)?"
    r"(?P<timezone>Z|(([-+])([0-9]{2}):([0-9]{2})))?)?)?)?"
)
TIMEZONE_REGEX = re.compile("(?P<prefix>[+-])(?P<hours>[0-9]{2}).(?P<minutes>[0-9]{2})")

class ParseError(Exception):
    """Raised when there is a problem parsing a date string"""

# Yoinked from python docs
ZERO = timedelta(0)
class Utc(tzinfo):
    """UTC
    
    """
    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO
UTC = Utc()

class FixedOffset(tzinfo):
    """Fixed offset in hours and minutes from UTC
    
    """
    def __init__(self, offset_hours, offset_minutes, name):
        self.__offset = timedelta(hours=offset_hours, minutes=offset_minutes)
        self.__name = name

    def utcoffset(self, dt):
        return self.__offset

    def tzname(self, dt):
        return self.__name

    def dst(self, dt):
        return ZERO
    
    def __repr__(self):
        return "<FixedOffset %r>" % self.__name

def local_timezone():
    import time
    t = time.time()
    dt_local = datetime.fromtimestamp(t)
    dt_utc = datetime.utcfromtimestamp(t)
    diff = (dt_local - dt_utc).total_seconds() / 3600.
    negative = diff < 0
    diff = abs(diff)
    hours = int(diff)
    minutes = int((diff-hours) * 60)
    name = '%02d:%02d' % (hours, minutes)
    if negative:
        hours = -hours
        minutes = -minutes
        name = '-' + name
    return FixedOffset(hours, minutes, name)

def parse_timezone(tzstring, default_timezone=None):
    """Parses ISO 8601 time zone specs into tzinfo offsets
    
    """
    if tzstring == "Z":
        return UTC
    # This isn't strictly correct, but it's common to encounter dates without
    # timezones so I'll assume the default (which defaults to UTC).
    # Addresses issue 4.
    if tzstring is None:
        return default_timezone
    m = TIMEZONE_REGEX.match(tzstring)
    if not m:
        raise ParseError("Unable to parse time zone string %r" % tzstring)
    prefix, hours, minutes = m.groups()
    hours, minutes = int(hours), int(minutes)
    if prefix == "-":
        hours = -hours
        minutes = -minutes
    return FixedOffset(hours, minutes, tzstring)

def parse_date(datestring, default_timezone=None):
    """Parses ISO 8601 dates into datetime objects
    
    The timezone is parsed from the date string. However it is quite common to
    have dates without a timezone. In this case the default timezone specified 
    in default_timezone, if any, is used.
    """
    if isinstance(default_timezone, basestring):
        if default_timezone.upper() == 'UTC':
            default_timezone = UTC
        elif default_timezone.lower() == 'local':
            tzinfo = local_timezone()
        else:
            default_timezone = parse_timezone(default_timezone)
    if not isinstance(datestring, basestring):
        raise ParseError("Expecting a string %r" % datestring)
    m = ISO8601_REGEX.match(datestring)
    if not m:
        raise ParseError("Unable to parse date string %r" % datestring)
    groups = m.groupdict()
    tz = parse_timezone(groups["timezone"], default_timezone=default_timezone)
    if groups["fraction"] is None:
        groups["fraction"] = 0
    else:
        groups["fraction"] = int(float("0.%s" % groups["fraction"]) * 1e6)
    return datetime(int(groups["year"]), int(groups["month"]), int(groups["day"]),
        int(groups["hour"]), int(groups["minute"]), int(groups["second"]),
        int(groups["fraction"]), tz)

def print_date(dt, default_timezone=None):
    if dt == 'now':
        dt = datetime.now()
        #default_timezone = local_timezone()
    s = '{dt.year}-{dt.month:02}-{dt.day:02}T{dt.hour:02}:{dt.minute:02}:{dt.second:02}.{dt.microsecond:06}'.format(dt=dt)
    tzinfo = dt.tzinfo
    if not tzinfo:
        if isinstance(default_timezone, basestring):
            if default_timezone.upper() == 'UTC':
                tzinfo = UTC
            elif default_timezone.lower() == 'local':
                tzinfo = local_timezone()
            else:
                tzinfo = parse_timezone(default_timezone)
        else:
            tzinfo = default_timezone
    if tzinfo:
        offset = tzinfo.utcoffset(dt).total_seconds()
        minutes = int(offset / 60)
        if minutes == 0:
            s += 'Z'
        else:
            hours = minutes / 60
            minutes = minutes % 60
            s += '{hours:+03}:{minutes:02}'.format(hours=hours,minutes=minutes)
    return s
        
        