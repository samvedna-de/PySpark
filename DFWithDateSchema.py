from pyspark.sql import *
from datetime import datetime
from pyspark.sql.types import *
import datetime
from pyspark.sql import SparkSession

spark=SparkSession.builder.master("local").appName("df").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
d=datetime.date(2025,7,16)
print("printing date")
print(d)
emp_dt=[['samvedna','data enginerr',8,datetime.date(2012,1,31)],['asmit','analyst',8,datetime.date(2011,7,16)],['aditya','new born',0,datetime.date(2025,7,16)]]
emp_dt_sch=StructType([StructField("Name",StringType(),True),StructField("Design",StringType(),True),StructField("Expr",IntegerType(),True),StructField("DOB",DateType(),True)])
emp_dt_df=spark.createDataFrame(emp_dt,emp_dt_sch)
print("Printing Schema for My DataFrame")
emp_dt_df.printSchema()
ew_added_col_df=added_col_df.select('loan_id','clnt_id',to_date(final_df.proc_run_dt,'yyyyMMdd').alias('proc_run_dt'),'src_appl_sys_cpnt_id','cre_ts','cre_usr_id')
print("Displaying DataFrame content")
emp_dt_df.show()
