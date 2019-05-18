from ctypes import CDLL, Structure, c_int, c_char_p, byref

libc = CDLL('libc.dylib')


class TM(Structure):
    _fields_ = [
        ("tm_sec", c_int),
        ("tm_min", c_int),
        ("tm_hour", c_int),
        ("tm_mday", c_int),
        ("tm_mon", c_int),
        ("tm_year", c_int),
        ("tm_wday", c_int),
        ("tm_yday", c_int),
        ("tm_isdst", c_int)
    ]


tm_struct = TM()

date_str = b'30-10-2016 16:18'
format_str = b'%d-%m-%Y %H:%M'

rez = libc.strptime(c_char_p(date_str), c_char_p(format_str), byref(tm_struct))

for field_name, field_type in tm_struct._fields_:
    print("{}: {}".format(field_name, getattr(tm_struct, field_name)))

print("strptime returned: %s" % repr(rez))
