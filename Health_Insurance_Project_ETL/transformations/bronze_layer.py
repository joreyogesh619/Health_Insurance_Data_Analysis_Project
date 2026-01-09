from pyspark import pipelines as dp
from pyspark.sql.functions import *

dp.create_streaming_table("bronze_company")

dp.create_auto_cdc_flow(
    target="bronze_company",
    source="health_insurance_project.health_insurance_schema.company",
    keys=["company_id"],
    sequence_by=col("created_date"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_employee")

dp.create_auto_cdc_flow(
    target="bronze_employee",
    source="health_insurance_project.health_insurance_schema.employee",
    keys=["employee_id"],
    sequence_by=col("created_date"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_claim")

dp.create_auto_cdc_flow(
    target="bronze_claim",
    source="health_insurance_project.health_insurance_schema.claim",
    keys=["claim_id"],
    sequence_by=col("processed_date"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_dependent")

dp.create_auto_cdc_flow(
    target="bronze_dependent",
    source="health_insurance_project.health_insurance_schema.dependent",
    keys=["dependent_id"],
    sequence_by=col("created_ts"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_enrollment")

dp.create_auto_cdc_flow(
    target="bronze_enrollment",
    source="health_insurance_project.health_insurance_schema.enrollment",
    keys=["enrollment_id"],
    sequence_by=col("enrollment_date"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_payment")

dp.create_auto_cdc_flow(
    target="bronze_payment",
    source="health_insurance_project.health_insurance_schema.payment",
    keys=["payment_id"],
    sequence_by=col("created_ts"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_plan")

dp.create_auto_cdc_flow(
    target="bronze_plan",
    source="health_insurance_project.health_insurance_schema.plan",
    keys=["plan_id"],
    sequence_by=col("created_ts"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

dp.create_streaming_table("bronze_policy")

dp.create_auto_cdc_flow(
    target="bronze_policy",
    source="health_insurance_project.health_insurance_schema.policy",
    keys=["policy_id"],
    sequence_by=col("created_date"),
    except_column_list=["source_system","etl_batch_id","created_ts","updated_ts","is_current_flag"],
    stored_as_scd_type="2"
)

# @dp.table(name="bronze_company",
#          comment="Raw company data from Delta source")
# def bronze_company():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.company")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_employee",
#          comment="Raw company data from Delta source")
# def bronze_employee():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.employee")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_policy",
#          comment="Raw company data from Delta source")
# def bronze_policy():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.policy")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_enrollment",
#          comment="Raw company data from Delta source")
# def bronze_enrollment():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.enrollment")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_claim",
#          comment="Raw company data from Delta source")
# def bronze_claim():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.claim")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_dependent",
#          comment="Raw company data from Delta source")
# def bronze_dependent():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.dependent")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_payment",
#          comment="Raw company data from Delta source")
# def bronze_payment():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.payment")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df

# @dp.table(name="bronze_plan",
#          comment="Raw company data from Delta source")
# def bronze_plan():
#     df = spark.readStream\
#     .format("delta")\
#     .table("health_insurance_project.health_insurance_schema.plan")
#     bronze_df=df.withColumn("ingestion_ts",current_timestamp())
#     return bronze_df