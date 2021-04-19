# Pooja_big_data_project
This repo is all about processing text data using Spark and Python.

## Author 
- Gundu Pooja

## Source of text
- https://www.gutenberg.org/
- I have choosen the plain text version of [The Hound of the Baskervilles by Arthur Conan Doyle](
https://www.gutenberg.org/files/2852/2852-0.txt)

## Tools / Languages:
- Languages : Python
- Tools: Databricks Notebook, Pyspark, Regex, Pandas, MatPlotLib, Seaborn, Urllib

## Process
## Gathering Data
1.To begin, we'll use the urllib.request library to request or pull data from the text data's url. After the data is pulled, it is stored in a temporary file called 'pooja.txt' and will get the text data from 'The Hound of Baskervilles' from gutenberg.org site.
```
# get text data from url
import urllib.request
stringInURL = "https://www.gutenberg.org/files/2852/2852-0.txt"
urllib.request.urlretrieve(stringInURL, "/tmp/pooja.txt")
```
2. Now that the data has been saved, we'll use dbutils.fs.mv to transfer the data in the temporary data to a new site called data.

```
dbutils.fs.mv("file:/tmp/pooja.txt", "dbfs:/data/pooja.txt")
```
3. Next, transfer the data file into Spark, using sc.textfile as shown below into Spark's RDD(Resilient distributed Systems),collection of elements that can be operated on in parallel. There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.

```
pooja_RDD = sc.textFile("dbfs:/data/pooja.txt")
```
## Cleaning the Data

4. The above data file contains the capitalized words, sentences, punctuations, and stopwords (words that do not add much meaning to a sentence Examples: the, have, etc.).
In the first step of cleaning the data,we will break down the data using flatMap, we will change any capitalized to a lower case, removing any spaces, and splitting up sentences into words.
```
# flatmap each line to words
wordsRDD = pooja_RDD.flatMap(lambda line : line.lower().strip().split(" "))
```
5. Next is the punctuations.For this we will import re(regular expression) library for anything that does not resemble a letter.
```
import re
# remove punctutation
clean_tokens_RDD = wordsRDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))
```
6. Next, is removing the stopwords if any, using pyspark.ml.feature by importing stopwordsRemover
```
#prepare to clean stopwords
from pyspark.ml.feature import StopWordsRemover
remove =StopWordsRemover()
stopwords = remove.getStopWords()
clean_words_RDD=clean_tokens_RDD.filter(lambda wrds: wrds not in stopwords)
```
## Processing the data
After cleaning the data, we can start processing the data.
7. we will map our words into intermediate key-value pairs. This will look like: (word,1), once we map it.
```
#maps the words to key value pairs
IKVPairsRDD= clean_words_RDD.map(lambda word: (word,1))
```
8. Next we will transform the pairs into a sort of word count. 
```
# reduceByKey() to get (word,count) results
pooja_word_count_RDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)
```
9. In the next step, we will use the sortbykey that lists the words in the descending order and then printing the top 25 most used words in 'The Hound of the Baskervilles'

```
pooja_results = pooja_word_count_RDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(25)
print(pooja_results)
```
10. Using the collect() function to retrieve all the elements from the dataset.
``` # collect() action to get back to python
results = pooja_word_count_RDD.collect()
print(results)
```

## Charting the Results 
10. we will use Pandas, MatPlotLib, and Seaborn to visualize.

```import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

source = 'The Hound of the Baskervilles, by Arthur Conan Doylesource'
title = 'Top Words in ' + source
xlabel = 'Count'
ylabel = 'Words'

# create Pandas dataframe from list of tuples
df = pd.DataFrame.from_records(pooja_results, columns =[xlabel, ylabel]) 
print(df)

# create plot (using matplotlib)
plt.figure(figsize=(10,4))
sns.barplot(xlabel, ylabel, data=df, palette="Paired").set_title(title)

```
## Charting result
- ![](https://github.com/GUNDUPOOJA/Pooja_big_data_project/blob/main/top25.PNG)

- ![](https://github.com/GUNDUPOOJA/Pooja_big_data_project/blob/main/charting.PNG)

References 
- https://github.com/thomastran7/big-data-final-project-tran
- https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/4574377819293972/2246755934805346/3186223000943570/latest.html


