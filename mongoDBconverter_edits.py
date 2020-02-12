# spotify recommender based on hdfs storage and previous songs (big data)

# import most pyspark commands
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions

def clean_Up(line):
    
    # txt file is , seperated
    holds = txt_file.split(",")
    
    return Row(year = int(holds[0]), 
               jan = int(holds[1]),
               feb = int(holds[2]),
               mar = int(holds[3]),
               apr = int(holds[4]),
               may = int(holds[5]),
               jun = int(holds[6]),
               jul = int(holds[7]),
               aug = int(holds[8]),
               sep = int(holds[9]),
               oct = int(holds[10]),
               nov = int(holds[11]),
               dec = int(holds[12]))
               

if __name__ == '__main__':
    
    # create spark session
    spark = SparkSession.builder.appName("ten_year_wage_data_to_mongoDB").getOrCreate()
    
    # raw pull (change to hdfs on live run)
    txt_file = spark.sparkContext.textFile("hdfs:///user/maria_dev/number_of_workers/total_nonfarm_monthly.txt")
    
    # remove ugly header line
    header = txt_file.first()
    txt_file = txt_file.filter(lambda line: line != header)
	
    # yearly wages pull
    # yearly_wages = txt_file.map(clean_Up)
    yearly_wages = txt_file.map(clean_Up).createDataFrame

    # convert to a DataFrame
    # yw_DF = spark.createDataFrame(yearly_wages)

    # pull to mongoDB
    yearly_wages.write\
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri","mongodb://127.0.0.1/numberworkers.users")\
        .mode('append')\
        .save()

    # draw out of mongoDB
    mongo_pull = spark.read\
    .format("com.mongodb.spark.sql.DefaultSource")\
    .option("uri","mongodb://127.0.0.1/numberworkers.users")\
    .load()

    # create view
    mongo_pull.createOrReplaceTempView("wage_data")
    
    # query this with spark sql
    sql_df = spark.sql("SELECT year, (jan + feb + mar + apr + may + jun + jul + aug + sep + oct + nov + dec)/12 as year_avg FROM wage_data GROUP BY year ORDER BY year_avg desc")    
    sql_df.show()

    # end session
    spark.stop()
    
    
    
   
        
    



