# percentage of missing observations in each column:
df_miss.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in df_miss.columns]).show()

# Outlier detection using IGR approach: [Q1-1.5*IQR, Q1+1.5*IQR]
df_outliers = spark.createDataFrame([ (1, 143.5, 5.3, 28),(2, 154.2, 5.5, 45), (3, 342.3, 5.1, 99), 
                                     (4, 144.5, 5.5, 33), (5, 133.2, 5.4, 54), (6, 124.1, 5.1, 21), 
                                     (7, 129.2, 5.3, 42),], 
                                    ['id', 'weight', 'height', 'age'])

cols = ['weight', 'height', 'age']
bounds = {}
for col in cols:
  quantiles = df_outliers.approxQuantile(col, [0.25, 0.75], 0.05 ) #0.05 is the approximation error, don't make it 0 else it will be too slow
  IQR = quantiles[1] - quantiles[0]
  bounds[col] = [
  quantiles[0] - 1.5 * IQR, quantiles[1] + 1.5 * IQR]
  
# Use it to flag outliers
outliers = df_outliers.select(*['id'] + [ (
  (df_outliers[c] < bounds[c][0]) |
  (df_outliers[c] > bounds[c][1]) ).alias(c + '_o') for c in cols 
  ])

df_outliers = df_outliers.join(outliers, on='id') 
df_outliers.filter('weight_o').select('id', 'weight').show() 
df_outliers.filter('age_o').select('id', 'age').show()

#______________ Append/Union a list of dataframes _____________
dfs = [df1, df2, df3, df4]

def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]
      
concat_df = union_all(dfs)



#_____________ Append (or union) a list of pyspark dataframes __________
from functools import reduce
from pyspark.sql import DataFrame

def union_list(dfs: Iterable[ps.DataFrame]) -> ps.DataFrame:
  """
  Function to append (union) dataframes stored in a list
  Args
  -----
  dfs (Iterable[ps.DataFrame]): A list containing pyspark dataframes
  
  Return
  ------
  df (ps.DataFrame): A single dataframe that contains the appended dataframes
  """
  
  df = reduce(DataFrame.unionAll, dfs)
  
  return df

concat_df = union_list(dfs) # Call the function


#__________________ Custom UDF ___________________________
# Suppose df contains a column - star_tating with integer values between 1 - 6
+-----------+-----------+
|marketplace|star_rating|
+-----------+-----------+
|         jp|          1|
|         de|          4|
|         fr|          5|
|         de|          5|
|         fr|          4|
+-----------+-----------+

# python udf
def star_rating_description(v_star_rating):
	if v_star_rating == 1:
		return "Poor"
	elif v_star_rating == 2:
		return "Fair"
	elif v_star_rating == 3:
		return "Average"
	elif v_star_rating == 4:
		return "Good"
	else:
		return "Excellent"
  
from pyspark.sql.functions import udf,col
from pyspark.sql.types import StringType

# Convert python UDF "star_rating_description" to PySpark UDF "udf_star_desc". 
#Now we can use it in PySpark code. "StringType" is the return type of PySpark function.
udf_star_desc = udf(lambda x:star_rating_description(x),StringType() )

>>> df_shoes.withColumn("rating_description",udf_star_desc(col("star_rating"))).select("marketplace","star_rating","rating_description").distinct().show(5)
+-----------+-----------+------------------+
|marketplace|star_rating|rating_description|
+-----------+-----------+------------------+
|         FR|          1|              Poor|
|         DE|          3|           Average|
|         US|          1|              Poor|
|         US|          4|              Good|
|         UK|          3|           Average|
+-----------+-----------+------------------+


#_____________ Custom udf _________________
"""Spark doesn't understand numpy float type"""
import numpy as np
from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType

array_mean = udf(lambda x: float(np.mean(x)), FloatType())
df.select("longitude", array_mean("longitude").alias("avg")).show()
+--------------------+------------------+
|           longitude|     avg_longitude|
+--------------------+------------------+
|      [-80.9, -82.9]|             -81.9|
|[-82.92, -82.93, ...|-82.93166666666667|
|    [-82.93, -82.93]|            -82.93|
+--------------------+------------------+


#__________MELT Pypsark dataframe ________________
def melt(df,cols,alias=('key','value')):
  other = [col for col in df.columns if col not in cols]
  for c in cols:
    df = df.withColumn(c, F.expr(f'map("{c}", cast({c} as double))'))
  df = df.withColumn('melted_cols', F.map_concat(*cols))
  return df.select(*other,F.explode('melted_cols').alias(*alias))

  
#_____________ Convert json to columns _________________
/+---+--------------------------------------------------------------------------+
//|id |value                                                                     |
//+---+--------------------------------------------------------------------------+
//|1  |{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}|
//+---+--------------------------------------------------------------------------+

 # Method 1: json_tuple

from pyspark.sql.functions import json_tuple
df.select(col("id"),json_tuple(col("value"),"Zipcode","ZipCodeType","City")) \
    .toDF("id","Zipcode","ZipCodeType","City") \
    .show(truncate=False)

//+---+-------+-----------+-----------+
//|id |Zipcode|ZipCodeType|City       |
//+---+-------+-----------+-----------+
//|1  |704    |STANDARD   |PARC PARQUE|
//+---+-------+-----------+-----------+

  # Method 2: get_json_object
from pyspark.sql.functions import get_json_object
df.select(col("id"),get_json_object(col("value"),"$.ZipCodeType").alias("ZipCodeType")) \
    .show(truncate=False)

//+---+-----------+
//|id |ZipCodeType|
//+---+-----------+
//|1  |STANDARD   |
//+---+-----------+
  
  
#____________________ FORWARD AND BACKWARD FILL ______________________
window = Window.partitionBy('name')\
               .orderBy('timestamplast')\
               .rowsBetween(-sys.maxsize, 0) # this is for forward fill  
               # .rowsBetween(0,sys.maxsize) # this is for backward fill  

# define the forward-filled column
filled_column = last(df['longitude'], ignorenulls=True).over(window)  # this is for forward fill  
# filled_column = first(df['longitude'], ignorenulls=True).over(window)  # this is for backward fill

df = df.withColumn('mmsi_filled', filled_column) # do the fill

  
#____________ CHECK IF DATAFRAMES ARE EQUAL __________
from pyspark.sql import DataFrame

def df_are_equal(d1: DataFrame, d2: DataFrame) -> bool:
    """Returns true iff the two input dataframes have the same rows, regardless of their order.
    This method takes into account duplicate rows, e.g. if x is a row, then [x, x] is not considered equal
    to [x].
    Note that an advantage of this method is that it avoids potentially expensive calls to `collect()`"""
    return d1.exceptAll(d2).count() == 0 and d2.exceptAll(d1).count() == 0

  

#__________________ SELECT NUMERICAL COLUMNS ________
numeric_cols = [field.name for field in df.schema.fields if isinstance(field.dataType, LongType) | isinstance(field.dataType, IntegerType) | isinstance(field.dataType, FloatType)]
  
#_______________ COUNT UNIQUE ITEMS IN A DATAFRAME __________
 def count_unique(df: ps.DataFrame) -> ps.DataFrame:
  """
  Function to count the number of unique items in each column of a dataframe
  """
  
  result = df.agg(*(f.countDistinct(f.col(c)).alias(c) for c in df.columns))
  return result

#____________ Clear matplotlib plots _______________
from IPython.display import set_matplotlib_formats
set_matplotlib_formats('retina')
  

#_____________ PAD DATAFRAMES ____________________
def pad_dates(self):
    start_date = self.df.agg(f.min('date').alias('min_date')).collect()[0]['min_date']
    end_date = self.df.agg(f.max('date').alias('max_date')).collect()[0]['max_date']
    padded_df = spark.createDataFrame([(1,)], ["dummy"])
    padded_df = padded_df.withColumn('date', f.explode(f.expr(f"sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)")))
    padded_df = padded_df.crossJoin(self.df.select(self.var_id, 'first_login').distinct())
    padded_df = (padded_df.filter(f.col('date')>=f.col('first_login'))
                           .drop('dummy', 'first_login')
                )

    self.df = (self.df.join(padded_df, on=[self.var_id, 'date'], how='right')
                       .fillna(0)
              )
