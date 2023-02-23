#==============================================================================#
#     Types of Item Similarity Measures for RecSys using PySpark
#==============================================================================#

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.mllib.linalg.distributed import IndexedRow, IndexedRowMatrix
from pyspark.mllib.linalg import Vectors


df_app = spark.createDataFrame([('1001', 'app_1'), ('1001', 'app_2'), ('1001', 'app_3'), ('1002', 'app_1'), 
                                ('1002', 'app_4'), ('1003', 'app_1'), ('1004', 'app_1'), ('1004', 'app_4')],
                                ['userid', 'appid'])


# 1. COSINE SIMILARITY
"""
Cosine similarity measures the angle between two vectors. It can be easily calculated by calling the Spark’s native function.
"""

def get_pd_norm():
  mat = IndexedRowMatrix(df_matrix.rdd.map(lambda row: IndexedRow(row[0], Vectors.dense(row[1:]))))
  array_norm = mat.toRowMatrix().computeColumnSummaryStatistics().normL2()
  pdf_norm = pd.DataFrame(array_norm, columns=["norm"]).reset_index()
  return mat, pdf_norm

mat, pdf_norm = get_pd_norm()

# get cosine similarity
df_cosine_similarity = mat.columnSimilarities().entries.toDF()
pdf_similarity = df_cosine_similarity.toPandas()
pdf_similarity = pdf_similarity.rename({'value': 'cosine_similarity'}, axis=1)

# merge norm
pdf_similarity = pdf_similarity.merge(pdf_norm, left_on="i", right_on="index", how="left")
pdf_similarity = pdf_similarity.rename(columns={"norm": "norm_i"})
pdf_similarity = pdf_similarity.drop(["index"], axis=1)

pdf_similarity = pdf_similarity.merge(pdf_norm, left_on="j", right_on="index", how="left")
pdf_similarity = pdf_similarity.rename(columns={"norm": "norm_j"})
pdf_similarity = pdf_similarity.drop(["index"], axis=1)





# 2. DOT PRODUCT
"""
Dot product is cosine similarity multiplied by the euclidean magnitudes of the two vectors. It can be understood as the projection of one vector on the 
other vector. The euclidean magnitude of a vector measures the popularity of the app among the users. When more users install an app, 
the euclidean magnitude of this app will become larger. Thus, unlike cosine similarity, dot product is affected by the popularity of the two apps. 
Dot product of a popular app and an unpopular app will be small, as if we imagine when the short vector is projected on the long vector, the projection length won’t be long. 
"""

pdf_similarity['dot_product'] = pdf_similarity['cosine_similarity'] * pdf_similarity["norm_i"] * pdf_similarity["norm_j"]





# 3. JACCARD SIMILARITY
"""
Jaccard similarity is the intersaction of the user sets divided by the union of the user sets who install the two apps. 
The intersaction of the user sets measures the similarity of the two apps, while the union of the user sets measures the diversity of the two apps. 
"""
pdf_similarity["jaccard_similarity"] = pdf_similarity['dot_product'] / (pdf_similarity["norm_i"] ** 2 + pdf_similarity["norm_j"] ** 2 - pdf_similarity['dot_product'])





# 4. CONDITIONAL PROBABILITY LIFT
"""
Conditional probability lift measures to what extent the installment of app B helps with the installment of app A.
"""
vector_length = df_matrix.count()
pdf_similarity["cond_probability"] = vector_length * pdf_similarity["cosine_similarity"] / (pdf_similarity["norm_i"] * pdf_similarity["norm_j"])  # P(A|B)/P(A)




#     FINAL RESULTS OF THE SIMILARITY MEASURES
#==============================================================================#
# map i,j to app names

app_list = df_matrix.columns
app_list.remove("userid")
pd_app_list = pd.DataFrame(app_list, columns=["app_id"])
pd_app_list["i"] = pd_app_list.index

# merge to i
pdf_similarity = pdf_similarity.merge(pd_app_list, on="i", how="left")
pdf_similarity = pdf_similarity.rename(columns={"app_id": "app_id_i"})

# merge to j
pd_app_list = pd_app_list.rename(columns={"i": "j"})
pdf_similarity = pdf_similarity.merge(pd_app_list, on="j", how="left")
pdf_similarity = pdf_similarity.rename(columns={"app_id": "app_id_j"})
pdf_similarity = pdf_similarity[['app_id_i', 'app_id_j', 'cosine_similarity', 'dot_product', 'cond_probability', 'jaccard_similarity']]

