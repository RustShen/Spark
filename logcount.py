# logcount.py
from __future__ import print_function

from pyspark import SparkContext
from operator import add
from pyspark.sql import Row

import time
import sys
import re

if __name__ == "__main__":

    sc = SparkContext("local", "Simple App")
# Enter log file absolute path
    logpath = '%s' % sys.argv[1]
    logRDD = sc.textFile(logpath)

# Create regular expression pattern to parse log messages' mainbody.
    PATTERN = '^(\S*\s\S*\s\S*)(.*)'

    def parseLogLine(logline):
        match = re.search(PATTERN, logline)
        return (Row(
            date_time=match.group(1),
            mainbody=match.group(2),
        ), 1)

    def parseLogs():
        parsed_logs = logRDD.map(parseLogLine).cache()
        access_logs = parsed_logs.filter(
            lambda s: s[1] == 1).map(lambda s: s[0]).cache()
        failed_logs = parsed_logs.filter(
            lambda s: s[1] == 0).map(lambda s: s[0]).cache()
        print('Reading %d messages' % parsed_logs.count())
        print('successfully parsed %d messages' % access_logs.count())
        print('failed to parse %d messages' % failed_logs.count())
        return parsed_logs, access_logs, failed_logs
    parsed_logs, access_logs, failed_logs = parseLogs()
    error_logs = (access_logs.map(lambda log: log.mainbody)
                             .filter(lambda s: "ERROR" in s)
                             .cache())
    warning_logs = (access_logs.map(lambda log: log.mainbody)
                               .filter(lambda s: "WARNING" in s)
                               .cache())
# Pairwise count each lines,sort by descending order.
    errorline = (error_logs.map(lambda x: (x, 1))
                           .reduceByKey(add)
                           .map(lambda (k, v): (v, k))
                           .sortByKey(ascending=False)
                           .collect())
    warningline = (warning_logs.map(lambda x: (x, 1))
                   .reduceByKey(add)
                   .map(lambda (k, v): (v, k))
                   .sortByKey(ascending=False)
                   .collect())

# Save all error and warning messages in files.
    e = open('./Error' + str(time.time()) + '.txt', 'w')
    e.write('From : %s \n' % logpath)
    e.write('There are %d error messages' % error_logs.count())
    errorlst = list()
    for i in errorlst:
        errorline.append((i[1], i[0]))
    e.write('\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), errorline)))
    e.close()
    w = open('./Warning' + str(time.time()) + '.txt', 'w')
    w.write('From : %s \n' % logpath)
    w.write('There are %d warning messages' % warning_logs.count())
    warninglst = list()
    for i in warninglst:
        warningline.append((i[1], i[0]))
    w.write('\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c),
                          warningline)))
    w.close()
