from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("readCSV").getOrCreate()
games = spark.read.csv('game.csv', header=True)

platform = games.groupBy('Platform').agg(fc.count('ID').alias('count'))
platform = platform.orderBy(fc.desc('count'))

year = games.groupBy('Year_of_Release').agg(fc.count('ID').alias('count'))
year = year.orderBy(fc.desc('count'))

genre = games.groupBy('Genre').agg(fc.count('ID').alias('count'))
genre = genre.orderBy(fc.desc('count'))

publisher = games.groupBy('Publisher').agg(fc.count('ID').alias('count'))
publisher = publisher.orderBy(fc.desc('count'))

year.show(10)
