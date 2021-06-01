import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import datetime




def main(spark):
	
	print("Spark Sessio %s",spark)
	df1 = spark.read.json('/Users/taffazzel.hossain/data-eng-exercise/input_source_1')
	df2 = spark.read.csv('/Users/taffazzel.hossain/data-eng-exercise/input_source_2',header='true')
	df1_renamed = df1.select(col('message'),col('post_id').alias('df1_post_id'),col('shares.count').alias('df1_share_count'))
	df2_renamed = df2.select(col('post_id').alias('df2_post_id'),col('comments'),col('likes'))
	joined_df = df1_renamed.join(df2_renamed, df1_renamed.df1_post_id==df2_renamed.df2_post_id,'inner')
	joined_df2 = joined_df.repartition(1)
	foldername = datetime.datetime.now().strftime('%Y-%m-%d:%H-%M-%S')
	joined_df2.write.format('csv').option('header', True).mode('overwrite').option('sep', ',').save("file:///Users/taffazzel.hossain/data-eng-exercise/" + foldername)
	
if __name__=="__main__":
	spark = SparkSession.builder.master("local[*]").appName('DataEngPractice').getOrCreate()
	main(spark)

