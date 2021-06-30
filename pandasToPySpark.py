# 1._________________ CREATE RDD ________________________

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

import pyspark.sql.functions as F
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

