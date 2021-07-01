# 1._________________ CREATE RDD ________________________________
#. ------------------ Tutorial websites -------------------------
https://hackersandslackers.com/structured-streaming-in-pyspark
https://runawayhorse001.github.io/LearningApacheSpark/pyspark.pdf       
# ----------------------------------------------------------------

from pyspark.sql import SparkSession
spark = SparkSession \
        .builder \
        .appName("Python Spark create RDD example") \
        .config("spark.some.config.option", "some-value") \
      .getOrCreate()

# 1.1
myData = spark.sparkContext.parallelize([(1,2), (3,4), (5,6), (7,8), (9,10)])
myData.collect()

# 1.2 Create using dataframe
Employee = spark.createDataFrame([ ('1', 'Joe', '70000', '1'), ('2', 'Henry', '80000', '2'), ('3', 'Sam', '60000', '2'),('4', 'Max', '90000', '1')],
                                ['Id', 'Name', 'Sallary','DepartmentId'])
Employee.show()

# 1.3 Create from database
## User information
user = 'your_username'
pw = 'your_password'
## Database information
table_name = 'table_name'
url = 'jdbc:postgresql://##.###.###.##:5432/dataset?user='+user+'&˓→password='+pw
properties = {'driver': 'org.postgresql.Driver', 'password': pw,'user': user}

df = spark.read.jdbc(url=url, table=table_name, properties=properties)
df.show(5)
df.printSchema()


#____________________ 2.0 rdd.DataFrame vs pd.DataFrame ___________________
## Create dataframe

#### From list
my_list = [['a', 1, 2], ['b', 2, 3],['c', 3, 4]]
col_name = ['A', 'B', 'C']

# caution for the columns=
pd.DataFrame(my_list, columns= col_name)
#
spark.createDataFrame(my_list, schema = col_name).show()

#### From Dict
d = {'A': [0, 1, 0], 'B': [1, 0, 1], 'C': [1, 0, 0]}

pd.DataFrame(d)
spark.createDataFrame(np.array(list(d.values())).T.tolist(),list(d.keys())).show()

## Import data from csv
dp = pd.read_csv('Advertising.csv')
ds = spark.read.csv(path='Advertising.csv', # sep=',', # encoding='UTF-8',# comment=None,header=True, 
                    inferSchema=True)

## Import data from json
dp = pd.read_json("data/data.json")
ds = spark.read.json('data/data.json')


## First n rows
dp[['id','timestamp']].head(4)
ds[['id','timestamp']].show(4)

## Column Names
dp.columns
ds.columns

## Data types
dp.dtypes
ds.dtypes

## _____________________Fill Null_____________________
my_list = [['male', 1, None], ['female', 2, 3],['male', 3, 4]]
dp = pd.DataFrame(my_list,columns=['A', 'B', 'C'])
ds = spark.createDataFrame(my_list, ['A', 'B', 'C'])

dp.fillna(-99)
ds.fillna(-99).show()

## _____________________Replace Values_____________________
# caution: you need to chose specific col
dp.A.replace(['male', 'female'],[1, 0], inplace=True)
ds.na.replace(['male','female'],['1','0']).show() #caution: Mixed type replacements are not supported

## _____________________Rename Columns_____________________
### 1. Rename all columns
dp.columns = ['a','b','c','d']
ds.toDF('a','b','c','d').show(4)

### 2. Rename one or more columns
mapping = {'Newspaper':'C','Sales':'D'}

dp.rename(columns=mapping).head(4)
new_names = [mapping.get(col,col) for col in ds.columns]
ds.toDF(*new_names).show(4)

ds.withColumnRenamed('Newspaper','Paper').show(4)


## _____________________Drop Columns_____________________
drop_name = ['Newspaper','Sales']
dp.drop(drop_name,axis=1).head(4)
ds.drop(*drop_name).show(4)

## _____________________Filter_____________________
dp = pd.read_csv('Advertising.csv')
ds = spark.read.csv(path='Advertising.csv', header=True, inferSchema=True)

dp[dp.Newspaper<20].head(4)
ds[ds.Newspaper<20].show(4)

dp[(dp.Newspaper<20) & (dp.TV>100)].head(4)
ds[(ds.Newspaper<20) & (ds.TV>100)].show(4)


##_____________________ With New Column_____________________
dp['tv_norm'] = dp.TV/sum(dp.TV)
dp.head(4)

import pyspark.sql.functions as F
ds.withColumn('tv_norm', ds.TV/ds.groupBy().agg(F.sum("TV")).collect()[0][0]).show(4)

dp['cond'] = dp.apply(lambda c: 1 if ((c.TV>100)&(c.Radio<40)) else 2 if c.Sales> 10 else 3, axis=1)
#
ds.withColumn('cond',
                        F.when((ds.TV>100) & (ds.Radio<40), 1)\
                        .when(ds.Sales>10, 2)\
                        .otherwise(3))
                        .show(4)

dp['log_tv'] = np.log(dp.TV)
dp.head(4)


ds.withColumn('log_tv', F.log(ds.TV)).show(4)


dp['tv+10'] = dp.TV.apply(lambda x: x+10)
dp.head(4)

ds.withColumn('tv+10', ds.TV+10).show(4)


## _____________________JOIN_______________

leftp = pd.DataFrame({'A': ['A0', 'A1', 'A2', 'A3'], 'B': ['B0', 'B1', 'B2', 'B3'], 'C': ['C0', 'C1', 'C2', 'C3'], 'D': ['D0', 'D1', 'D2', 'D3']}, 
                     index=[0, 1, 2, 3])

rightp = pd.DataFrame({'A': ['A0', 'A1', 'A6', 'A7'], 'F': ['B4', 'B5', 'B6', 'B7'],'G': ['C4', 'C5', 'C6', 'C7'], 'H': ['D4', 'D5', 'D6', 'D7']},
                      index=[4, 5, 6, 7])

lefts = spark.createDataFrame(leftp)
rights = spark.createDataFrame(rightp)

### Left Join

leftp.merge(rightp, on='A', how='left')
lefts.join(rights, on='A', how='left').orderBy('A', ascending=True).show()

### Right Join

leftp.merge(rightp, on='A', how='right')
lefts.join(rights, on='A', how='right').orderBy('A', ascending=True).show()

### Inner Join

leftp.merge(rightp, on='A', how='inner')
lefts.join(rights, on='A', how='inner').orderBy('A', ascending=True).show()

### Full Join
leftp.merge(rightp,on='A',how='outer')
lefts.join(rights,on='A',how='full').orderBy('A',ascending=True).show()


# _________________ CONCAT COLUMNS ____________________
my_list = [('a', 2, 3), ('b', 5, 6), ('c', 8, 9), ('a', 2, 3), ('b', 5, 6), ('c', 8, 9)]
col_name = ['col1', 'col2', 'col3']
#
dp = pd.DataFrame(my_list, columns = col_name)
ds = spark.createDataFrame(my_list, schema=col_name)

dp['concat'] = dp.apply(lambda x:'%s%s'%(x['col1'], x['col2']), axis=1)
ds.withColumn('concat', F.concat('col1','col2')).show()

# _________________ GROUPBY ____________________
dp.groupby(['col1']).agg({'col2':'min','col3':'mean'})
ds.groupBy(['col1']).agg({'col2': 'min', 'col3': 'avg'}).show()

# ____________________Pivot____________________
pd.pivot_table(dp, values='col3', index='col1', columns='col2', aggfunc = np.sum)
ds.groupBy(['col1']).pivot('col2').sum('col3').show()

#______________ RANK AND DENSE RANK _____________
d ={'Id':[1,2,3,4,5,6],'Score': [4.00, 4.00, 3.85, 3.65, 3.65, 3.50]}
data = pd.DataFrame(d)

dp = data.copy()
ds = spark.createDataFrame(data)

dp['Rank_dense'] = dp['Score'].rank(method='dense', ascending =False)
dp['Rank'] = dp['Score'].rank(method='min', ascending =False)

#
import pyspark.sql.functions as F
from pyspark.sql.window import Window

w = Window.orderBy(ds.Score.desc())
ds = ds.withColumn('Rank_spark_dense',F.dense_rank().over(w))
ds = ds.withColumn('Rank_spark',F.rank().over(w))
ds.show()





#__________________________________________________________________________
##### DATA EXPLORATION
#__________________________________________________________________________
# selected varables for the demonstration
num_cols = ['Account Balance','No of dependents']



## Numerical Variable_________________________________________________
df.select(num_cols).describe().show()

def describe_pd(df_in, columns, deciles=False):
        '''
        Function to union the basic stats results and deciles
        :param df_in: the input dataframe
        :param columns: the cloumn name list of the numerical variable
        :param deciles: the deciles output
        :return : the numerical describe info. of the input dataframe
        '''
        if deciles:
                percentiles = np.array(range(0, 110, 10))
        else:
                percentiles = [25, 50, 75]
        
        percs = np.transpose([np.percentile(df_in.select(x).collect(), percentiles) for x in columns])
        percs = pd.DataFrame(percs, columns=columns)
        percs['summary'] = [str(p) + '%' for p in percentiles]
        spark_describe = df_in.describe().toPandas()
        new_df = pd.concat([spark_describe, percs],ignore_index=True)
        new_df = new_df.round(2)
        
        return new_df[['summary'] + columns]

describe_pd(df,num_cols)
describe_pd(df,num_cols,deciles=True)

## Histogram

var = 'Age (years)'
x = data1[var]
bins = np.arange(0, 100, 5.0)
plt.figure(figsize=(10,8))
# the histogram of the data
plt.hist(x, bins, alpha=0.8, histtype='bar', color='gold',
         ec='black',weights=np.zeros_like(x) + 100. / x.size)
plt.xlabel(var)
plt.ylabel('percentage')
plt.xticks(bins)
plt.show()
fig.savefig(var+".pdf", bbox_inches='tight')

## Box and Violin plot

x = df.select(var).toPandas()
fig = plt.figure(figsize=(20, 8))
ax = fig.add_subplot(1, 2, 1)
ax = sns.boxplot(data=x)
ax = fig.add_subplot(1, 2, 2)
ax = sns.violinplot(data=x)


## CATEGORICAL VARIABLE _______________________
### Frequency Table
from pyspark.sql import functions as F
from pyspark.sql.functions import rank, sum, col
from pyspark.sql import Window

window = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
# withColumn('Percent %',F.format_string("%5.0f%%\n",col('Credit_num')*100/col('total'))).\

tab = df.select(['age_class','Credit Amount']).\
        groupBy('age_class').\
        agg(    F.count('Credit Amount').alias('Credit_num'),
                F.mean('Credit Amount').alias('Credit_avg'),
                F.min('Credit Amount').alias('Credit_min'),
                F.max('Credit Amount').alias('Credit_max')).\
        withColumn('total', sum(col('Credit_num')).over(window)).\
        withColumn('Percent', col('Credit_num')*100/col('total')).\
        drop(col('total'))


# MULTIVARIATE ANALYSIS____________________
## Correlation matrix

from pyspark.mllib.stat import Statistics
import pandas as pd

corr_data = df.select(num_cols)
col_names = corr_data.columns
features = corr_data.rdd.map(lambda row: row[0:])
corr_mat = Statistics.corr(features, method = "pearson")
corr_df = pd.DataFrame(corr_mat)
corr_df.index, corr_df.columns = col_names, col_names
print(corr_df.to_string())

## Crosstabulation
df.stat.crosstab("age_class", "Occupation").show()


## Create New Columns and Populate the columns

from pyspark.sql.functions import lit, when, col

df = df_with_winner.withColumn('testColumn', 
                               F.lit('this is a test')) # add a column, and populate each cell in that column with occurrences of the string: this is a test.
display(df)

# Construct a new dynamic column
df = df_with_test_column.withColumn('gameWinner', 
                         when((col("homeFinalRuns") > col("awayFinalRuns")), col("homeFinalRuns"))
                                .otherwise(lit('awayTeamName')))

display(df)


## Filter with like, contains(), startswith(), and endsWith()
df = df.filter(df.winner.like('Nat%'))

## isin() Match multiple values
df = df.filter(df.gameWinner.isin('Cubs', 'Indians'))


## concat() For Appending Strings

df = df.withColumn("gameTitle",
        concat(df.homeTeamName, lit(" vs. "), df.awayTeamName))



# DROPPING ROWS______________________
## Drop NA
df = df.dropna(subset=['postal_code', 'city', 'country', 'address_1'])
display(df)

## Drop duplicates
df = df.dropduplicates(subset="recall_number")
display(df)


# Filtering by String Values ______________

df.filter(df.city.contains('San Francisco'): #Returns rows where strings of a column contain a provided substring. In our example, filtering by rows which contain the substring "San Francisco" would be a good way to get all rows in San Francisco, instead of just "South San Francisco".
df.filter(df.city.startswith('San')): #Returns rows where a string starts with a provided substring.
df.filter(df.city.endswith('ice')): #Returns rows where a string starts with a provided substring.
df.filter(df.city.isNull()): #Returns rows where values in a provided column are null.
df.filter(df.city.isNotNull()): #Opposite of the above.
df.filter(df.city.like('San%')): #Performs a SQL-like query containing the LIKE clause.
df.filter(df.city.rlike('[A-Z]*ice$')): #Performs a regexp filter.
df.filter(df.city.isin('San Francisco', 'Los Angeles')):# Looks for rows where the string value of a column matches any of the provided strings exactly.


## Filter by date________
df = df.filter(df.report_date.between('2013-01-01 00:00:00','2015-01-11 00:00:00')

## Sort dataframe
df = df.orderBy('report_date', ascending=False)
               
## Rename columns
df = df.withColumnRenamed('recall_number', 'id')

### We can also change multiple columns at once:
df = df.selectExpr("product_type as type", "product_description as product_description")
