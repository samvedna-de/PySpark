import sys
from pyspark.sql import SparkSession\n",
from pyspark.sql.functions import count"

if __name__ ==  "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount mnmdataset.csv" , file=sys.stderr)
        sys.exit(-1)

   
   spark=(SparkSession
          .builder
          .appName("PythonMnMCount")
          .getOrCreate())
   
   mnfile=sys.argv[1]

   mnmdf = (spark.read.format("csv").option("header", "true").option("inferschema","true").load(mnfile))
   countmn_df = (mnmdf.select("State","Color","Count").groupBy("State","Color").
                agg(count("Count").alias("Total")).orderBy("Total",ascending=False))

  countmn_df.show(n=60, truncate=False)
  print("Total Rows %d" %countmn_df.count())

  spark.stop()
  
