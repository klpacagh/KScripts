
import sys
import decimal
import time
import math
# import datetime
from datetime import datetime

ts = '1997-08-12 00:00:00'
datetime_object = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

print(datetime_object)

def toInternal(dt) -> int:
    if dt is not None:
        seconds = (
            calendar.timegm(dt.utctimetuple()) if dt.tzinfo else time.mktime(dt.timetuple())
        )
        return int(seconds) * 1000000 + dt.microsecond
            
def fromInternal(ts: int) -> datetime:
    if ts is not None:
        # using int to avoid precision loss in float
        return datetime.fromtimestamp(ts // 1000000).replace(microsecond=ts % 1000000)
            
temp = fromInternal(toInternal(datetime_object))
print(temp)

