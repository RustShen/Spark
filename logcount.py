from pyspark import SparkContext
from operator import add
from pyspark.sql import Row
import sys
import re


#Create log file RDD
sc = SparkContext("local", "Simple App")
logRDD = sc.textFile(log)

#create regular expression pattern to parse log messages' mainbody.
#then count the number of messages,error messages and warning messages.
PATTERN = '^(\S*\s\S*)(.*)'
def parseLogLine(logline):
    match = re.search(PATTERN, logline)
    return (Row(
        date_time = match.group(1),
        mainbody = match.group(2),
    ), 1)

def parseLogs():
        parsed_logs = logRDD.map(parseLogLine).cache()
        access_logs = parsed_logs.filter(lambda s: s[1] == 1).map(lambda s: s[0]).cache()
        failed_logs = parsed_logs.filter(lambda s: s[1] == 0).map(lambda s: s[0]).cache()
        print 'Read %d messages, successfully parsed %d messages, failed to parse %d messages' % (parsed_logs.count(), access_logs.count(), failed_logs.count())
        return parsed_logs, access_logs, failed_logs,
parsed_logs, access_logs, failed_logs = parseLogs()

error_logs = access_logs.map(lambda log:log.mainbody).filter(lambda s : "ERROR" in s).cache()
warning_logs = access_logs.map(lambda log:log.mainbody).filter(lambda s : "WARNING" in s).cache()
print 'There are %d error messages and %d warning messages' % (error_logs.count() , warning_logs.count())



#Pairwise count each lines,sort by descending order.
errorline = (error_logs.map(lambda x : (x,1))
                       .reduceByKey(add)
                       .map(lambda (k,v):(v,k))
                       .sortByKey(ascending=False)
                       .collect())
warningline = (warning_logs.map(lambda x : (x,1))
                           .reduceByKey(add)
                           .map(lambda (k,v):(v,k))
                           .sortByKey(ascending=False)
                           .collect())

#save all error and warning messages in descengding order.

e = open("./lognameError.txt",'w')
errorlst = list()
for i in errorlst:
    errorline.append((i[1],i[0]))
print >> e, '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), errorline))
e.close()

w = open("./lognameWarning.txt",'w')
warninglst = list()
for i in warninglst:
    warningline.append((i[1],i[0]))
print >> w, '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), warningline))
w.close()
