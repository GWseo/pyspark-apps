from pyspark.sql.functions import col, broadcast, count
from pyspark.sql import SparkSession
import time
"""
[조건]
v1. 프로그램명 : homework/ch10_8/wide_transform.py 
2. join 수행시 skills DataFrame을 broadcast 할 것
3. 최종 데이터프레임 컬럼: ['skill_name','job_count'] (job_count 기준 내림차순 정렬)
4. 최종 데이터 프레임의 count() 로 건수 확인
v5. 프로그램 종료 전 sleep(1200) 수행
v6. 아래 파라미터 부여 
        .config('spark.sql.adaptive.enabled', 'false') \
        .config('spark.executor.cores', '2') \
        .config('spark.executor.memory', '2g') \
        .config('spark.executor.instances', '3') \
"""


spark = SparkSession \
        .builder \
        .appName('wide_transform') \
        .config('spark.sql.adaptive.enabled', 'false') \
        .config('spark.executor.cores', '2') \
        .config('spark.executor.memory', '2g') \
        .config('spark.executor.instances', '3') \
        .getOrCreate()


job_skills_path = 'hdfs:///home/spark/sample/linkedin_jobs/jobs/job_skills.csv'
job_skills_schema = 'job_id LONG, skill_abr STRING'
skills_path =  'hdfs:///home/spark/sample/linkedin_jobs/mappings/skills.csv'
skills_schema = 'skill_abr STRING, skill_name STRING'



job_skills_df = spark.read \
                 .option('header','true') \
                 .option('multiLine','true') \
                 .schema(job_skills_schema) \
                 .csv(job_skills_path)

skills_df = spark.read \
                .option('header','true') \
                .option('multiLine','true') \
                .schema(skills_schema) \
                .csv(skills_path)

#2. join 수행시 skills DataFrame을 broadcast 할 것
join_df = job_skills_df.join(
    other = broadcast(skills_df),
    on = ['skill_abr'],
    how = 'semi'
)

group_df = join_df.groupBy('skill_name').agg(count('job_id').alias('job_count'))

select_df = group_df.select('skill_name', 'job_count')
select_df = select_df.orderBy('job_count')
print(select_df.show())
print(select_df.count())

time.sleep(1200)