
import time
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql import functions as F
from datetime import datetime, timedelta
from pyspark.sql import types as T


start = time.time()
config = configparser.ConfigParser()
config.read('C:/Users/Priyanshi Gupta/Documents/Data Engineer NanoDegree/Capstone/Code/dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark



def process_airport_data(spark, input_data, output_data):
    
    #Read data from source 
    df=spark.read.format('csv').options(header='true').load(input_data)
    
    #creating Airport dimension table 
    Dim_airport = df.select("ident", "type", "iata_code", "name", "iso_country","iso_region","municipality","gps_code","coordinates")
    Dim_airport.createOrReplaceTempView("airport_table")
    
    Dim_airport = spark.sql("""
                            SELECT DISTINCT ident, 
                            type, iata_code, name, iso_country,iso_region,municipality,gps_code,coordinates
                            FROM airport_table 
                            
                        """)
    
    Dim_airport.createOrReplaceTempView("Dim_airport_table")
    
    
    ## write Airport dimension table to parquet files partitioned by region into data lake
    Dim_airport.write.partitionBy("iso_region").mode('overwrite').parquet(os.path.join(output_data, 'Airport'))
    
    
def process_us_demographic_data(spark, input_data, output_data):
    
    #Read data from source 
    df=spark.read.format('csv').options(inferSchema="true",delimiter=";",header="true").load(input_data)
    
    #creating us-cities-demographics dimension table 
    Stage_us_demo = df.select("City","State","Median Age","Male Population","Female Population","Total Population","Number of Veterans","Foreign-born","Average Household Size","State Code","Race","Count")
    Stage_us_demo.createOrReplaceTempView("Stage_us_demo_table")
    
    Dim_us_demo = spark.sql("""SELECT DISTINCT City,State,`Median Age`as Median_Age
    
    ,`Male Population` as Male_Population,`Female Population` as Female_Population,`Total Population` as Total_Population,
    `Number of Veterans` as Number_of_Veterans,`Foreign-born` as Foreign_born,`Average Household Size` as Average_Household_Size,
    `State Code` as State_Code FROM Stage_us_demo_table """)
    
    Dim_us_demo.createOrReplaceTempView("Dim_us_demo_table")
    
    
    ## write us-cities-demographics dimension table to parquet files partitioned by State into data lake
    Dim_us_demo.write.partitionBy("State").mode('overwrite').parquet(os.path.join(output_data, 'US_Demographic'))
    
    #creating us-cities-demographics_by_Race dimension table 
    Dim_us_demo_by_race = spark.sql("""SELECT DISTINCT City,State,race,count FROM Stage_us_demo_table """)

    Dim_us_demo_by_race.createOrReplaceTempView("Dim_us_demo_by_race_table")

    ## write us-cities-demographics_by_race dimension table to parquet files partitioned by State into data lake
    Dim_us_demo_by_race.write.partitionBy("State").mode('overwrite').parquet(os.path.join(output_data, 'US_Demographic_by_Race'))

    
    
def  process_i94_data(spark, input_data, output_data):
    
    #load data from source
    df=spark.read.parquet(input_data)
    
    #1) Converting Dates to proper format
    def convert_datetime(x):
        try:
            start = datetime(1960, 1, 1)
            return start + timedelta(days=int(x))
        except:
            return None
        
    udf_datetime_from_sas = udf(lambda x: convert_datetime(x), T.DateType())
    
    df=df.withColumn("arrival_date", udf_datetime_from_sas("arrdate")) \
        .withColumn("departure_date", udf_datetime_from_sas("depdate"))
    
    
    
    #creating i94 stage table 
    Stage_i94 = df.select("cicid","i94yr","i94mon","arrdate","depdate","arrival_date","departure_date","i94cit","i94res","i94port","i94mode","i94addr","i94bir","i94visa","visapost","entdepa","entdepd","entdepu","matflag","biryear","gender","airline","fltno","visatype")
    
    
    Stage_i94=Stage_i94.withColumn("i94yr", Stage_i94['i94yr'].cast('int')).withColumn("i94mon", Stage_i94['i94mon'].cast('int'))\
              .withColumn("i94mode", Stage_i94['i94mode'].cast('int')).withColumn("biryear", Stage_i94['biryear'].cast('int'))\
              .withColumn("i94cit", Stage_i94['i94cit'].cast('int')).withColumn("i94res", Stage_i94['i94res'].cast('int'))\
              .withColumn("arrdate", Stage_i94['arrdate'].cast('int')).withColumn("depdate", Stage_i94['depdate'].cast('int'))
    
    Stage_i94.createOrReplaceTempView("Stage_i94_table")
    
    

    
    
    
    
    #creating Time dimension from i94 stage
    Dim_time = spark.sql("""SELECT DISTINCT arrdate as dateid, arrival_date as date,cast(i94yr as int) as year,cast(i94mon as int) as month FROM Stage_i94_table 
                           
     union
                        SELECT DISTINCT depdate as dateid,departure_date as date,cast(i94yr as int) as year,cast(i94mon as int) as month FROM Stage_i94_table""")
    
    Dim_time=Dim_time.withColumn('day',dayofmonth(Dim_time.date)).withColumn('weekday',date_format(col("date"), "EEEE"))\
                     .withColumn('weekday',date_format(col("date"), "EEEE")).withColumn("weeek_num", date_format(col("date"), "W"))\
                                                         
    
    Dim_time.createOrReplaceTempView("Dim_time_table")
    
    ## write Time dimension table to parquet files partitioned by State into data lake
    Dim_time.write.partitionBy("i94yr").mode('overwrite').parquet(os.path.join(output_data, 'Time'))

    
    
    
    
    #Creating Status dimension from i94 stage
    Dim_status = spark.sql("""SELECT DISTINCT entdepa,entdepd,entdepu,matflag FROM Stage_i94_table""")                                       
    Dim_status = Dim_status.withColumn("statusid", F.monotonically_increasing_id())
    
    Dim_status.createOrReplaceTempView("Dim_status_table")
    
    ## write Status dimension table to parquet files partitioned by State into data lake
    Dim_status.write.mode('overwrite').parquet(os.path.join(output_data, 'Status'))

    

    
    #Creating arrival_port dimension from i94 stage
    Dim_arrival_port = spark.sql("""SELECT DISTINCT i94port,i94addr FROM Stage_i94_table""")

                                       
    Dim_arrival_port = Dim_arrival_port.withColumn("arrival_portid", F.monotonically_increasing_id()) 
    Dim_arrival_port.createOrReplaceTempView("Dim_arrival_port_table")
    
    ## write Status dimension table to parquet files partitioned by State into data lake
    Dim_arrival_port.write.mode('overwrite').parquet(os.path.join(output_data, 'Arrival_port'))

    
    
     #Creating Flight_detail dimension from i94 stage
    Dim_flight_dtl = spark.sql("""SELECT DISTINCT airline,fltno FROM Stage_i94_table""")

                                       
    Dim_flight_dtl = Dim_flight_dtl.withColumn("fltid", F.monotonically_increasing_id()) 
    Dim_flight_dtl.createOrReplaceTempView("Dim_flight_dtl_table")
    
    ## write Flight dimension table to parquet files partitioned by State into data lake
    Dim_flight_dtl.write.mode('overwrite').parquet(os.path.join(output_data, 'Flight'))
  

    
    
    #Create Fact from dimension
    Fact_i94_table = spark.sql("""
    select distinct cicid,statusid,arrival_portid,fltid,src.i94addr as state_code, arrdate, depdate,i94mode,i94visa,visatype
    from
    (SELECT * from Stage_i94_table )src
    LEFT join
    (select * from Dim_status_table)stat
    ON src.entdepa <=> stat.entdepa AND
    src.entdepd <=> stat.entdepd AND
    src.entdepu <=> stat.entdepu AND
    src.matflag <=> stat.matflag
    
    left join
    (select * from Dim_arrival_port_table)arvl
    ON src.i94port <=> arvl.i94port
    and src.i94addr <=> arvl.i94addr
    
    left join
    (select * from Dim_flight_dtl_table)flt
    ON src.airline <=> flt.airline
    and src.fltno <=> flt.fltno
    """)
    
    ## write Flight dimension table to parquet files partitioned by State into data lake
    Fact_i94_table.write.partitionBy("state_code").mode('overwrite').parquet(os.path.join(output_data, 'i94_data'))
    
    
    



    
    
     
    
    
def main():
    spark = create_spark_session()
    
    output_data = "s3a://capstone-data-lake/"
    
    #input_data = "/user/Capstone-data/airport-codes_csv.csv"
    input_data = "C:/Users/Priyanshi Gupta/Documents/Data Engineer NanoDegree/Capstone/Dataset/airport-codes_csv.csv"
    process_airport_data(spark, input_data, output_data)    
    
    input_data = "C:/Users/Priyanshi Gupta/Documents/Data Engineer NanoDegree/Capstone/Dataset/us-cities-demographics.csv"
    #input_data = "/user/Capstone-data/us-cities-demographics.csv"
    process_us_demographic_data(spark, input_data, output_data)    
    
    #input_data = "/user/Capstone-data/i94/part-*.parquet"
    input_data = "C:/Users/Priyanshi Gupta/Documents/Data Engineer NanoDegree/Capstone/Dataset/i94/part-*.parquet"
    process_i94_data(spark, input_data, output_data)
    
    end = time.time()
    
    print(str(end - start)+"Program end time")
    


if __name__ == "__main__":
    main()


    

    
    
    