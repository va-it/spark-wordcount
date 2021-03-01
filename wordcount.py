import sys
import math

from pyspark.sql import SparkSession, DataFrame, functions as F, Window

def lower_and_clean_string(string_to_edit):
        # define a list of all punctuation needed to be removed
        punctuation_to_remove='!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~'
        # Replace dashes with spaces and make lowercase 
        lowercased_string = string_to_edit.replace('-',' ').lower()
        # loop over each mark to remove and subsitute it with empty space in string
        for character in punctuation_to_remove:
            lowercased_string = lowercased_string.replace(character, '')
        return lowercased_string

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    # read the text from the file, and create an RDD of lines
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    
    # remove puntuaction marks, separate dashed words and make them lowercase
    lines = lines.map(lower_and_clean_string)

    # first we generate a flat map of single words
    # then we add a 1 'counter' to each word
    # then we "group by" word and sum the 1 added before for each entry (word, X)
    word_and_frequency_pairs = lines.flatMap(lambda words: words.split(' ')) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a + b) \

    # frequency_and_word_pairs = word_and_frequency_pairs.map(lambda x: (x[1],x[0]))

    word_and_frequency_pairs = word_and_frequency_pairs.sortBy(lambda a: a[1], ascending=False)

    print(word_and_frequency_pairs.take(100))

    columns = ["Word","Frequency"]

    df = word_and_frequency_pairs.toDF(columns)

    df.show(100)


    # # collect (?)
    # output = counts.collect()

    # # create a spark dataframe (table)
    # df = spark.createDataFrame(output, ("Word", "Frequency"))

    # # Add Rank (row index) column
    # w = Window().orderBy(F.col("Frequency").desc(),F.col("Word").asc())
    # df = df.withColumn("Rank", F.row_number().over(w))

    # # calculate 5 percent of total records
    # first_5_percent = math.ceil((counts.count())*(5/100))


    # # display the table .show(truncate=False) not working...
    # df.orderBy(F.col("Frequency").desc(),F.col("Word").asc()).show(first_5_percent)

    # # temp. Show each word
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))

    # print("total {}".format(df.count()))

    spark.stop()