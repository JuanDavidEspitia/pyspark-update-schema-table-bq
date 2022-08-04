from operator import add
import pyspark
sc = pyspark.SparkContext('local[*]')

data = sc.parallelize(list("Hello World"))
counts = data.map(lambda x:
                  (x, 1)).reduceByKey(add).sortBy(lambda x: x[1],
                                                  ascending=False).collect()

for (word, count) in counts:
    print("{}: {}".format(word, count))