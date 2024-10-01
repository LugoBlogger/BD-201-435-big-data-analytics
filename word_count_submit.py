from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (SparkSession
  .builder
  .master("local[*]")   # optional
  .appName(
    "Analyzing the vocabulary of Pride and Prejudice.")
  .getOrCreate())

# to avoid print everything
spark.sparkContext.setLogLevel("WARN")

results = (
  # spark.read.text("./data/gutenberg_books/1342-0.txt")
  spark.read.text("./data/gutenberg_books/*.txt")
  .select(F.split(F.col("value"), " ").alias("line"))
  .select(F.explode(F.col("line")).alias("word"))
  .select(F.lower(F.col("word")).alias("word"))
  .select(F.regexp_extract(F.col("word"), "[a-z']*", 0).alias("word"))
  .where(F.col("word") != "")
  .groupby("word")
  .count()
)

# Show the top 10 of the most occurrence words in Jane Austen - Pride and Prejudice
print(results.orderBy("count", ascending=False).show(10))

# Write the results into csv file
# You need to remove the existed path if you run this command for the second time.
results.write.csv("./data/vocab_count.csv")