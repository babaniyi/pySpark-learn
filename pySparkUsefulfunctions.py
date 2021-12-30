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
