# cython: language_level=3
from io import BytesIO

from libc.stdio cimport printf, sprintf
from libc.time cimport tm, strptime, time_t, gmtime, strftime
from libc.stdlib cimport exit, EXIT_FAILURE


cdef extern from "<time.h>" nogil:
    time_t timegm(tm *)
    time_t timelocal(tm *)

cdef extern from "regex.h" nogil:
    ctypedef struct regmatch_t:
        int rm_so
        int rm_eo
    ctypedef struct regex_t:
        pass
    int REG_EXTENDED, REG_ICASE
    int regcomp(regex_t*preg, const char*regex, int cflags)
    int regexec(const regex_t *preg, const char *string, size_t nmatch,
                regmatch_t pmatch[], int eflags)
    size_t regerror(int errcode, const regex_t *preg, char *errbuf, size_t errbuf_size)
    void regfree(regex_t*preg)

cdef char *re_search(char *row, char *regex) nogil:
    cdef int r = 0
    cdef regex_t reg
    cdef regmatch_t match[2]
    cdef char result[30]
    cdef int l
    r = regcomp(&reg, regex, REG_ICASE | REG_EXTENDED)
    if r != 0:
        regfree(&reg)
        printf('problem1')
        return ''

    r = regexec(&reg, row, 2, match, 0)
    l = match[1].rm_eo - match[1].rm_so
    if r == 0:
        sprintf(result, "%.*s", l, row + match[1].rm_so)
    else:
        printf('problem: %d %s', r, row)
    regfree(&reg)
    return result

ctypedef tm TM

cdef char *extract_date(char *s, char *fmt) nogil:
    cdef TM tm
    cdef TM *tmp
    cdef time_t t
    cdef char outstr[11]

    strptime(s, fmt, &tm)
    t = timelocal(&tm)
    tmp = gmtime(&t)

    if strftime(outstr, sizeof(outstr), '%Y-%m-%d', tmp) == 0:
        exit(EXIT_FAILURE)

    return outstr

cpdef char *get_date(char *row, char *regex, char *fmt) nogil:
    cdef char *s
    cdef char *date
    with nogil:
        s = re_search(row, regex)
        date = extract_date(s, fmt)
    return date

cdef list _indexer(bytes buf, long long offset, bytes datetime_regex, bytes datetime_format):
    stream = BytesIO(buf)
    cdef list lines = []
    cdef char *date

    for line in stream:
        date = get_date(line, datetime_regex, datetime_format)
        lines.append((date, offset + stream.tell()))

    return lines

def indexer(args, datetime_regex, datetime_format):
    buf, offset = args
    return _indexer(buf, offset, datetime_regex, datetime_format)
