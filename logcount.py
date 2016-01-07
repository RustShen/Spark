from pyspark import SparkContext
from operator import add

#Create log file RDD, count log lines.
sc = SparkContext("local", "Simple App")
logRDD = sc.textFile(log)
print logRDD.count()

#Create error and warning lines RDD, count error and warning lines.
errorRDD = logRDD.filter(lambda x : "error" in x)
                 .cache()
warningRDD = logRDD.filter(lambda x :"warning" in x)
                   .cache()

print errorRDD.count()
print warningRDD.count()

#Pairwise count each lines,sort by descending order.
errorline = errorRDD.map(lambda x : (x,1))
                    .reduceByKey(add)
                    .map(lambda (k,v):(v,k))
                    .sortByKey(ascending=False)
                    .collect()
warningline = warningRDD.map(lambda x : (x,1))
                        .reduceByKey(add)
                        .map(lambda (k,v):(v,k))
                        .sortByKey(ascending=False)
                        .collect()

#print all error and warning lines in descengding order.
errorlst = list()
for i in errorlst:
    errorline.append((i[1],i[0]))
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), errorlst))

warninglst = list()
for i in warninglst:
    warningline.append((i[1],i[0]))
print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), warninglst))
