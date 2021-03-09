import sys
import math
import pandas as pd

from pyspark.sql import SparkSession, DataFrame, functions as F, Window

def clean_string(string_to_clean):
        # define a list of all punctuation needed to be removed
        punctuation_to_remove='!"#$%&\'()*+-,./:;<=>?@[\\]^_`{|}~'
        # loop over each mark to remove and subsitute it with empty space in string
        for character in punctuation_to_remove:
            string_to_clean = string_to_clean.replace(character, '')
        return string_to_clean

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
    
    # remove puntuaction marks
    lines = lines.map(clean_string)
    
    # first we generate a flat map of single lowercase words
    words = lines.flatMap(lambda words: words.split(' ')) \
                 .map(lambda word: word.lower())  
    
    print('Total number of words: {}'.format(words.count()))
    print('Total number of distinct words: {}'.format(words.distinct().count()))
    
    
    # then we add a 1 'counter' to each word
    # then we "group by" word and sum the 1 added before for each entry (word, X)
    word_and_frequency_pairs = words.map(lambda word: (word, 1)) \
                                    .reduceByKey(lambda a, b: a + b) \

    # convert the RDD into a DataFrame temporarily used to further convert into a Pandas DataFrame
    columns = ["Word","Frequency"]
    df = word_and_frequency_pairs.toDF(columns)

    # convert the dataFrame into a Pandas dataframe for easy sorting
    pandasDataframe = df.toPandas()
    pandasDataframeSorted = pandasDataframe.sort_values(by=['Frequency', 'Word'], ascending=[False, True])

    # and then convert back to a PySpark DataFrame
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
    
    
    
    
    # Section below is pretty much a copy and paste from above. Most of the logic should move into a function
    print('----------------------')
    
    # extract letters from each word and convert them to lowercase
    # we have the words RDD from before
    letters = words.flatMap(lambda word: [character for character in word]) \
                   .map(lambda letter: letter.lower())
    
    print('Total number of letters: {}'.format(letters.count()))
    print('Total number of distinct letters: {}'.format(letters.distinct().count()))
    
    letter_and_frequency_pairs = letters.map(lambda letter: (letter, 1)) \
                                    .reduceByKey(lambda a, b: a + b) \

    # convert the RDD into a DataFrame temporarily used to further convert into a Pandas DataFrame
    columns = ["Letter","Frequency"]
    df = letter_and_frequency_pairs.toDF(columns)
    
    # convert the dataFrame into a Pandas dataframe for easy sorting
    pandasDataframe = df.toPandas()
    pandasDataframeSorted = pandasDataframe.sort_values(by=['Frequency', 'Letter'], ascending=[False, True])

    # and then convert back to a PySpark DataFrame
    pySparkDataFrame = spark.createDataFrame(pandasDataframeSorted)

    # Add Rank (row index) column
    w = Window().orderBy(F.col("Frequency").desc(),F.col("Letter").asc())
    pySparkDataFrame = pySparkDataFrame.withColumn("Rank", F.row_number().over(w))   
    
    pySparkDataFrame.show(100)

 

    # *************** STOP ****************
    spark.stop()