import sys
from operator import add

from pyspark.sql import SparkSession, DataFrame


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)
    
    output = counts.collect()

    df = spark.createDataFrame(output, ('Word', 'Count'))
    df.show(n=2)

    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()