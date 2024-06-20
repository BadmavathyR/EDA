"""
Created by Badmavathy at 6:43 PM 6/18/2024 
"""
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EDA with PySpark on 50GB Dataset") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Sample path to a small part of Common Crawl data
sample_wet_file = "s3a://commoncrawl/crawl-data/CC-MAIN-2022-05/segments/1642366445124.0/wet/CC-MAIN-20220117002725-20220117032725-00000.warc.wet.gz"

# Load a sample WET file
wet_df = spark.read.text(sample_wet_file)

# Display the first few rows
wet_df.show(5, truncate=False)

# Count the number of lines in the WET file
line_count = wet_df.count()
print(f"Number of lines: {line_count}")

# Sample the dataset to work with a manageable subset
sampled_wet_df = wet_df.sample(withReplacement=False, fraction=0.01, seed=42)

# Show some basic statistics
sampled_wet_df.describe().show()

# Split lines into words
words_df = sampled_wet_df.select(explode(split(sampled_wet_df.value, " ")).alias("word"))

# Filter out empty words and calculate word frequencies
word_freq_df = words_df.filter(words_df.word != "").groupBy("word").count().orderBy("count", ascending=False)

# Show the most common words
word_freq_df.show(10)

# Convert the word frequency DataFrame to Pandas for visualization
word_freq_pd_df = word_freq_df.limit(20).toPandas()

# Visualization: Top 20 most common words
plt.figure(figsize=(12, 8))
sns.barplot(x='count', y='word', data=word_freq_pd_df)
plt.title('Top 20 Most Common Words')
plt.xlabel('Frequency')
plt.ylabel('Word')
plt.show()

# Stop Spark session
spark.stop()



