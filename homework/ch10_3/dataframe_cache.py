from pyspark.sql.functions import col

path1 ='hdfs:///home/spark/sample/linkedin_jobs/companies/company_industries.csv'
path2 ='hdfs:///home/spark/sample/linkedin_jobs/companies/employee_counts.csv'

df_c_indust = spark.read.option("header","true").option('multiLine','true').csv(path1)
df_e_count = spark.read.option("header","true").option('multiLine','true').csv(path2)

print(df_c_indust.count())
print(df_e_count.count())

df_c_indust_f = df_c_indust.filter(col('industry') == 'IT Services and IT Consulting')
df_join = df_c_indust_f.join(df_e_count, df_c_indust_f.company_id == df_e_count.company_id, inner)
df_join2 = df_join.filter(col('employee_count') >= 1000).orderBy(col('employee_count').desc())
df_join2.show()

sleep(120)