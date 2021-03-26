import sys
import math
import pandas as pd
import re
import os.path

from pyspark.sql import SparkSession, DataFrame, functions as F, Window

def printTotalsInformation(entities, distinct_entities, entity):
        print('Total number of {entity}: {count}'.format(entity=entity,count=entities.count()))
        print('Total number of distinct {entity}: {count}'.format(entity=entity,count=distinct_entities.count()))
        
def calculateThresholds(distinct_entities):
    count = distinct_entities.count()
    popular_threshold = math.ceil(count*(5/100))
    common_threshold_l = math.floor(count*(47.5/100))
    common_threshold_u = math.ceil(count*(52.5/100))
    rare_threshold = count - math.ceil(count*(5/100))
    return {
        'popular_threshold': popular_threshold, 
        'common_threshold_l': common_threshold_l, 
        'common_threshold_u': common_threshold_u,
        'rare_threshold': rare_threshold
    }

def printThresholds(thresholds):
    print('Popular treshold: {}'.format(thresholds['popular_threshold']))
    print('Lower common treshold: {}'.format(thresholds['common_threshold_l']))
    print('Upper common treshold: {}'.format(thresholds['common_threshold_u']))
    print('Rare treshold: {}'.format(thresholds['rare_threshold']))

def categoriseAndPrintEntities(distinct_entities, dataFrame, entity):
    popular_entities = dataFrame.filter(dataFrame.Rank.between(1,thresholds['popular_threshold']))
    common_entities = dataFrame.filter(dataFrame.Rank.between(thresholds['common_threshold_l'],thresholds['common_threshold_u']))
    rare_entities = dataFrame.filter(dataFrame.Rank.between(thresholds['rare_threshold'],distinct_entities.count()))
    
    print('\nPopular {}'.format(entity))
    popular_entities.show(n=popular_entities.count())
    print('Common {}'.format(entity))
    common_entities.show(n=common_entities.count())
    print('Rare {}'.format(entity))
    rare_entities.show(n=rare_entities.count())
    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)
    
    if not os.path.isfile(sys.argv[1]):
        print("Please use a valid file", file=sys.stderr)
        sys.exit(-1)
        
    # generate output file name    
    outputFile = 'output-{}'.format(sys.argv[1])
    
    # save a reference to the sys.stdout so it can be restored once execution ends 
    original = sys.stdout
    
    # redirect any print statement to a file
    sys.stdout = open(outputFile, 'w')

    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    # read the text from the file, and create an RDD of lines
    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    
    lines = lines.map(lambda line: line.encode('latin','ignore').decode('latin'))
    
    # first we generate a flat map of lowercase words separated by space or (some) punctuation
    
    words = lines.flatMap(lambda words: re.split('([.,:;!?"—_\[\]\(\)\{\}\s]+)', words)) \
                 .map(lambda word: word.lower()) \
                 .filter(lambda word: word != '') 
    
    # remove saxon genitive from words ('s) - https://stackoverflow.com/a/46289237
    words = words.map(lambda word: re.sub(r"(\w+)'s", r'\1s', word))
    
    # remove all words with numbers in them
    words = words.filter(lambda word: not re.match('.*[\d].*', word))
    
    # remove all words with symbols but not hyphenated ones
    words = words.filter(lambda word: not re.match('.*[^a-z-].*', word))
    
    # ^([-].*$) remove all words beginning with hyphen (-abc)
    # (.*[-]{2,}.*$) remove all words with multiple hyphens next to each other (abc--abc)
    # ^(.*[^a-z]$) remove all words with ending symbol (abc-)(abc...)
    words = words.filter(lambda word: not re.match('^([-].*$)|(.*[-]{2,}.*$)|^(.*[^a-z]$)', word))

    distinct_words = words.distinct()

    #print totals
    printTotalsInformation(words, distinct_words, 'words')
    
    #calculate thresholds
    thresholds = calculateThresholds(distinct_words)

    # print thresholds
    printThresholds(thresholds)
    
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
    
    categoriseAndPrintEntities(distinct_words, pySparkDataFrame, 'words')

    print('----------------------\n')
    
    # extract letters from each word and convert them to lowercase
    # we have the words RDD from before
    letters = words.flatMap(lambda word: [character for character in word]) \
                   .filter(lambda letter: letter != '-')
    
    distinct_letters = letters.distinct()
    
    # print totals
    printTotalsInformation(letters, distinct_letters, 'letters')
    
    # calculate thresholds
    thresholds = calculateThresholds(distinct_letters)
    
    # print thresholds
    printThresholds(thresholds)
    
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

    categoriseAndPrintEntities(distinct_letters, pySparkDataFrame, 'letters')
 
    # *************** STOP ****************
    spark.stop()
    
    # restore stdout to its original form
    sys.stdout = original