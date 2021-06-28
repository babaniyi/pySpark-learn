# 1._________________ Create RDD ________________________

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
#
spark.createDataFrame(np.array(list(d.values())).T.tolist(),list(d.keys())).show()

## Import data from csv
dp = pd.read_csv('Advertising.csv')
ds = spark.read.csv(path='Advertising.csv', # sep=',', # encoding='UTF-8',# comment=None,header=True, 
                    inferSchema=True)

## Import data from json
dp = pd.read_json("data/data.json")
ds = spark.read.json('data/data.json')


