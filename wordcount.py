import sys
from operator import add

from pyspark.sql import SparkSession, DataFrame, functions as F, Window

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda words: words.split(' ')) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b) \

    output = counts.collect()

    df = spark.createDataFrame(output, ('Word', 'Count'))

    # Add Rank (+1 index)
    w = Window().orderBy("count")
    df = df.withColumn('Rank', F.row_number().over(w))

    df.show()

    for (word, count) in output:
        print("%s: %i" % (word, count))

    spark.stop()