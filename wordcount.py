import sys
import math
import pandas as pd

from pyspark.sql import SparkSession, DataFrame, functions as F, Window

def lower_and_clean_string(string_to_edit):
        # define a list of all punctuation needed to be removed
        punctuation_to_remove='!"#$%&\'()*+-,./:;<=>?@[\\]^_`{|}~'
        # loop over each mark to remove and subsitute it with empty space in string
        for character in punctuation_to_remove:
            string_to_edit = string_to_edit.replace(character, '')
        return string_to_edit

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

    columns = ["Word","Frequency"]

    df = word_and_frequency_pairs.toDF(columns)

    pandasDataframe = df.toPandas()
    pandasDataframeSorted = pandasDataframe.sort_values(by=['Frequency', 'Word'], ascending=[False, True])
    pandasDataframeSorted.reset_index(drop=True, inplace=True)
    pandasDataframeSorted.index += 1

    print(pandasDataframeSorted)

    pySparkDataFrame = spark.createDataFrame(pandasDataframeSorted)

    # Add Rank (row index) column
    w = Window().orderBy(F.col("Frequency").desc(),F.col("Word").asc())
    pySparkDataFrame = pySparkDataFrame.withColumn("Rank", F.row_number().over(w))   
    
    pySparkDataFrame.show(100)

    # # calculate 5 percent of total records
    five_percent = math.ceil((pySparkDataFrame.count())*(5/100))
    
    popular_words = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(1,five_percent))
    
    # how to get the records ranked between 47.5% and 52.5% ???
    common_words = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(21,24))
    
    # how to get the last 5% of records???
    rare_words = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(40,43))
    
    print('Popular words')
    popular_words.show()
    
    print('Common words')
    common_words.show()
    
    print('Rare words')
    rare_words.show()
    
    # stop the Spark Session
    spark.stop()
 

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