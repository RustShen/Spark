from __future__ import print_function
import sys
import re
from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

PATTERN = '^(\S*\s\S*)(.*)'
def parseLogLine(logline):
    match = re.search(PATTERN, logline)
    return (Row(
        date_time = match.group(1),
        mainbody = match.group(2),
    ), 1)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: flume_wordcount.py <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingFlumeWordCount")
    ssc = StreamingContext(sc, 1)

    hostname, port = sys.argv[1:]
    kvs = FlumeUtils.createStream(ssc, hostname, int(port))
    lines = kvs.map(lambda x: x[1])
    Errorcounts = (lines.map(parseLogLine)
    	           .filter(lambda s: s[1] == 1)
    	           .map(lambda s: s[0].mainbody)   	         
    	           .filter(lambda s : "ERROR" in s)
    	           .map(lambda log: (log, 1))
    	           .reduceByKey(lambda a, b : a + b))

    Warningcounts = (lines.map(parseLogLine)
    	           .filter(lambda s: s[1] == 1)
    	           .map(lambda s: s[0].mainbody)   	         
    	           .filter(lambda s : "WARNING" in s)
    	           .map(lambda log: (log, 1))
    	           .reduceByKey(lambda a, b : a + b))
    Errorcounts.pprint()
    Warningcounts.pprint()

    ssc.start()
    ssc.awaitTermination()
