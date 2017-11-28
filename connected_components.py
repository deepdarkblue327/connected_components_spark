# coding: utf-8
# python 2.7

# suniluma

# Sunil 
# Umasankar

# 50249002

import sys, operator, time
from pyspark import SparkConf, SparkContext

#Functions

#copied from CSE570 slides
def vertex(line):
    l = line.split(" ")
    v = [ int(x) for x in l ]
    return [(v[0],v[1]),(v[1],v[0])]

#Large and small star mapping functions
def processing(star_type):
    def _processing(key):
        m = min(key[0],min(key[1]))
        op = operator.gt if star_type == "SMALL" else operator.lt
        n_u = [(key[0],i) for i in key[1] if op(key[0],i)]
        return [(i[1],m) if i[1]!=m else (key[0],i[1]) for i in n_u]  if n_u else []
    return _processing


#Erasing old spark context
sc = SparkContext.getOrCreate()
sc.stop()

#Starts Here
conf = SparkConf().setAppName("suniluma")
sc = SparkContext(conf = conf)
lines = sc.textFile(sys.argv[1])

V = lines.flatMap(vertex)
count = 0
n = 1

#alternating between large and small star
while n != 0:
    prev_V = V
    if count%2 == 0:
        V = V.groupByKey().flatMap(processing("LARGE"))
    else:
        V = V.groupByKey().flatMap(processing("SMALL"))
    count += 1
    n = V.count()
    #print n

#adding root node of each component, to the component it belongs to in the final result
prev_V = prev_V.map(lambda x: (x[1],x[0])).groupByKey().map(lambda x: (x[0],x[0])).union(prev_V).distinct()

try:
    prev_V.map(lambda x: " ".join([str(x[0]),str(x[1])])).saveAsTextFile(sys.argv[2])
    output_dir = sys.argv[2]
except:
    prev_V.map(lambda x: " ".join([str(x[0]),str(x[1])])).saveAsTextFile("Output_"+str(int(time.time())))
    output_dir = str(int(time.time()))

sc.stop()
print("written into directory" + output_dir)