import sys
sys.path.insert(0, '.')
from pyspark.sql import SparkSession
from commons.Utils import Utils
import functools
import io
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, DateType, IntegerType
from pyspark.sql import functions as Fn
from pyspark.sql import *
from pyspark.sql.functions import isnan, when, count, col
import pyspark.sql as SQL
from pyspark_dist_explore import hist
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
#%matplotlib inline
if __name__ == "__main__":
     spark=SparkSession.builder.master("local[*]").appName("Read Excel").getOrCreate()
     spark.sparkContext.setLogLevel("ERROR")



schema=StructType([StructField("Date",StringType(),True),StructField("Name",StringType(),True),StructField("RefNo",StringType(),True),StructField("Value_dt",StringType(),True),StructField("WithdrawalAmount",StringType(),True),StructField("DepositeAmount",StringType(),True),StructField("ClosingBalance",StringType(),True)])

df = pd.read_excel('in/transact.xls', sheet_name='Sheet2',inferSchema='')

sdf=spark.createDataFrame(df,schema=schema)

new_sdf=sdf.select(to_date(sdf.Date,"dd/MM/yy").alias('Date'),'Name', 'RefNo',to_date(sdf.Value_dt,"dd/MM/yy").alias('Value_Dt'),sdf["WithdrawalAmount"].cast(DoubleType()),sdf["DepositeAmount"].cast(DoubleType()),sdf["ClosingBalance"].cast(DoubleType()))

#################Income Computation

income=new_sdf.where(isnan(col("WithdrawalAmount")))
income.groupBy().agg(Fn.sum("DepositeAmount")).collect()[0][0]
total_income=income.groupBy().agg(Fn.sum("DepositeAmount")).collect()[0][0]
print("\nYour income by months")
in_df=income.select(year(income.Date).alias("Year"), month(income.Date).alias("Months"), 'DepositeAmount')
in_df.groupBy("Year","Months").agg(Fn.sum("DepositeAmount").alias("Income_By_Month")).orderBy("Year","Months").show()
income=in_df.groupBy("Year","Months").agg(Fn.sum("DepositeAmount").alias("Income_By_Month")).orderBy("Year","Months")
print("\nYour total income in August/2019-2020")
in_df.agg(Fn.sum("DepositeAmount").alias('Total')).show()


#################Expenditure Computation

Expenditure=new_sdf.where(isnan(col("DepositeAmount")))
out_df=Expenditure.select(year(Expenditure.Date).alias("Year"), month(Expenditure.Date).alias("Months"), 'WithdrawalAmount')
expend=out_df.groupBy("Year","Months").agg(Fn.sum("WithdrawalAmount").alias("Spent_in_Months")).orderBy("Year","Months")
print("\nYour expenditure by months")
out_df.groupBy("Year","Months").agg(Fn.sum("WithdrawalAmount").alias("Spent_in_Months")).orderBy("Year","Months").show()
print("\nYour total expenditure in August/2019-2020")
out_df.agg(Fn.sum("WithdrawalAmount").alias('Total')).show()


#################Income Histogram Generation
print("\n\nYou Histogram of income is genereated and can be found at the current location...")
x=income.select('Months').rdd.flatMap(lambda x: x).collect()
y=income.select('Income_By_Month').rdd.flatMap(lambda x: x).collect()
df=pd.DataFrame({'Income' : y}, index=x)
df.index.name='Months'
ax=df.plot.bar(title='Income over 2019-2020')
ax.set_ylabel('Amount in Rs')
#plt.show()
ax.plot()
plt.savefig('Earned.png',bbox_inches='tight')

#################Expenditure Histogram Generation
print("\n\nYou Histogram of Expenditure is genereated and can be found at the current location...")
x=expend.select('Months').rdd.flatMap(lambda x: x).collect()
y=expend.select('Spent_in_Months').rdd.flatMap(lambda x: x).collect()
df=pd.DataFrame({'Expenditure' : y}, index=x)
df.index.name='Months'
ax=df.plot.bar(title='Expenses over 2019-2020')
ax.set_ylabel('Amount in Rs')
#plt.show()
ax.plot()
plt.savefig('Expense.png',bbox_inches='tight')

###################
new=[ Row(2019,8,0.0)]
sch=StructType([StructField("Year",IntegerType(),True),StructField("Months",IntegerType(),True),StructField("Spent_in_Months",DoubleType(),True)])
new1=spark.createDataFrame(new,sch)
appended=new1.union(expend)



x=income.select('Months').rdd.flatMap(lambda x: x).collect()
y1=income.select('Income_By_Month').rdd.flatMap(lambda x: x).collect()
y2=appended.select('Spent_in_Months').rdd.flatMap(lambda x: x).collect()

income.select('Income_By_Month').rdd.flatMap(lambda x: x).take(10)
appended.select('Spent_in_Months').rdd.flatMap(lambda x: x).take(10)

ycmap1 = ListedColormap(['#5B9BD5', '#ED7D31'])
ycmap2 = ListedColormap(['#FF6666', '#66FF66'])
df = pd.DataFrame({'Income': y1, 'Expenditure': y2}, index=x)
df.index.name = 'Months'
print(df.head())  # Show sample records

# Create a figure with 2 subplots and get their handles
fig, (ax0, ax1) = plt.subplots(2, 1)
#fig, (ax0) = plt.plots()

df.plot.bar(ax=ax0, figsize=(6.4, 6.4), cmap=ycmap1,title='Income vs Expenses over 2019-2020)')

ax0.set_ylabel('Amount in Rs')
ax0.plot()

df.plot.bar(ax=ax1, stacked=True, cmap=ycmap2,title='Income vs Expenses over 2019-2020)')
ax1.set_ylabel('Amount in Rs')
ax1.plot()
plt.tight_layout()  # To prevent overlapping of subplots
#plt.show()
plt.savefig('Merged.png',bbox_inches='tight')

