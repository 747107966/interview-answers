"""
作者：陶远红
日期：2024-03-28
名称：面试题
注意：该代码为测试环境，如果切到生产，需要根据业务需求进行更改
"""

from pyspark.sql import SparkSession
import os
import pyspark.sql.functions as F

from pyspark.sql.types import *

os.environ['SPARK_HOME'] = '/export/server/spark'
os.environ['PYSPARK_PYTHON'] = '/root/anaconda3/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/root/anaconda3/bin/python3'

# 1.创建spark对象
spark = SparkSession.builder.appName('interview_answer').master('local[*]').getOrCreate()

# 2.构建 RDD的数据集
data = [
    ('AE686(AE)', '7', 'AE686', 2022),
    ('AE686(AE)', '8', 'BH2740', 2021),
    ('AE686(AE)', '9', 'EG999', 2021),
    ('AE686(AE)', '10', 'AE0908', 2023),
    ('AE686(AE)', '11', 'QA402', 2022),
    ('AE686(AE)', '12', 'OA691', 2022),
    ('AE686(AE)', '12', 'OB691', 2022),
    ('AE686(AE)', '12', 'OC691', 2019),
    ('AE686(AE)', '12', 'OD691', 2017)
]

df = spark.createDataFrame(data, schema=['peer_id', 'id_1', 'id_2', 'year'])

df.printSchema()
df.show()


# 要求一：如果peer_id 包含了 id_2，则取年份，比如第0行,peer_id包含了id_2的值，则取year
# 先自定义一个函数,判断peer_id是否包含了id_2
def add_post(peer_id, id_2):
    return id_2 in peer_id


# 函数注册
add_post_dsl = spark.udf.register('add_post_sql', add_post, StringType())

new_df = df.select('peer_id', 'id_2', 'year', add_post_dsl('peer_id', 'id_2').alias('n1')).where("n1='true'").select(
    'peer_id', 'year')
new_df.show()

# 需求二：基于需求一获取到的年份,提取到小于等于需求一当中的年份数据

df2 = df.where(df['year'] <= new_df.first()['year']).groupBy('year').agg(F.count('peer_id').alias('cnt')).orderBy(
    'year', ascending=False)
df2.show()
df2.select('cnt').limit(2).agg(F.sum('cnt')).show()

# 通过number来判断需要那几个年份的数据,这里我们传固定值7给number
number = 7
i = 1
while True:
    df3 = df2.limit(i)
    df3.show()
    result = df3.select('cnt').agg(F.sum('cnt').alias('result')).first()['result']
    if result >= number:
        break
    else:
        i += 1
print(result)

# 需求三,基于第二步的结果,反批回df,获取到peer_id再去重
finally_df = df.join(df3, on='year', how='inner').select('peer_id', 'year').dropDuplicates()
finally_df.show()

# 把DataFrame转换成RDD对象
rdd = finally_df.rdd
tuple_rdd=rdd.map(lambda row:tuple(row))
tuple_data=tuple_rdd.collect()
print(tuple_data)