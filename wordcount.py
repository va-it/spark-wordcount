import sys
import math
import pandas as pd
import re

from pyspark.sql import SparkSession, DataFrame, functions as F, Window

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
    
    lines = lines.map(lambda line: line.encode('ascii','ignore'))
    
    lines = lines.map(lambda line: line.decode())

    # REGEX TO MATCH ALL PUNCTUATION MARKS AND SPACES [?!.,:;"\'—\[\]\(\)\{\}\s]+
    # Punctuation as defined here: https://punctuationmarks.org/
    # first we generate a flat map of lowercase words separated by space or punctuation
    words = lines.flatMap(lambda words: re.split('[?!.,:;"\'—\[\]\(\)\{\}\s]+', words)) \
                 .map(lambda word: word.lower()) \
                 .filter(lambda word: word != '')

    
    # Filter out all words with numbers and symbols in them
    # ([a-z]+[\d]+[a-z]+) catches any word that starts with letters and includes numbers (ab1c)
    # ([\d]+[a-z]+) catches any word that starts with numbers and includes letters (1abc)
    # ([a-z]+[\d]+) catches any word that starts with letters and ends with numbers (abc1)   
    
    words = words.filter(lambda word: not re.match('([a-z]+[\d]+[a-z]+)|([\d]+[a-z]+)|([a-z]+[\d]+)', word))
    
    # Filter out all words with symbols in them
    # ([a-z]+[^a-z]+[a-z]+) catches any word that starts with letters and includes symbols (ab$c)
    # ([^a-z]+[a-z]+) catches any word that starts with symbols and includes letters ($abc)
    # ([a-z]+[^a-z]) catches any word that starts with letters and ends with symbols (abc$)
    
    words = words.filter(lambda word: not re.match('([a-z]+[^a-z]+[a-z]+)|([^a-z]+[a-z]+)|([a-z]+[^a-z])', word))
    
    # Filter out any word made up of only digits or only symbols
    words = words.filter(lambda word: not re.match('([\d]+)|([^a-z]+)', word))
    
    
    print('Total number of words: {}'.format(words.count()))
    distinct_words = words.distinct().count()
    print('Total number of distinct words: {}'.format(distinct_words))
    
    # calculate thresholds
    popular_threshold = math.ceil(distinct_words*(5/100))
    common_threshold_l = math.ceil(distinct_words*(47.5/100))
    common_threshold_u = math.floor(distinct_words*(57.5/100))
    rare_threshold = distinct_words - math.ceil(distinct_words*(5/100))
    
    print('Popular treshold: {}'.format(popular_threshold))
    print('Lower common treshold: {}'.format(common_threshold_l))
    print('Upper common treshold: {}'.format(common_threshold_u))
    print('Rare treshold: {}'.format(rare_threshold))
    
    
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

    
    popular_words = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(1,popular_threshold))
    
    common_words = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(common_threshold_l,common_threshold_u))
    
    rare_words = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(rare_threshold,distinct_words))
    
    print('Popular words')
    popular_words.show(n=popular_words.count())
    
    print('Common words')
    common_words.show(n=common_words.count())
    
    print('Rare words')
    rare_words.show(n=rare_words.count())
    
    
    # Section below is pretty much a copy and paste from above. Most of the logic should move into a function
    print('----------------------')
    
    # extract letters from each word and convert them to lowercase
    # we have the words RDD from before
    letters = words.flatMap(lambda word: [character for character in word]) \
                   .filter(lambda letter: letter != '-')
    
    print('Total number of letters: {}'.format(letters.count()))
    distinct_letters = letters.distinct().count()
    print('Total number of distinct letters: {}'.format(distinct_letters))
    
    # calculate thresholds
    popular_threshold = math.ceil(distinct_letters*(5/100))
    common_threshold_l = math.ceil(distinct_letters*(47.5/100))
    common_threshold_u = math.floor(distinct_letters*(57.5/100))
    rare_threshold = distinct_letters - math.ceil(distinct_letters*(5/100))
    
    print('Popular treshold: {}'.format(popular_threshold))
    print('Lower common treshold: {}'.format(common_threshold_l))
    print('Upper common treshold: {}'.format(common_threshold_u))
    print('Rare treshold: {}'.format(rare_threshold))
    
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
    
    popular_letters = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(1,popular_threshold))

    common_letters = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(common_threshold_l,common_threshold_u))

    rare_letters = pySparkDataFrame.filter(pySparkDataFrame.Rank.between(rare_threshold,distinct_letters))
    
    print('Popular letters')
    popular_letters.show(n=popular_letters.count())
    
    print('Common letters')
    common_letters.show(n=common_letters.count())
    
    print('Rare letters')
    rare_letters.show(n=rare_letters.count())
 

    # *************** STOP ****************
    spark.stop()