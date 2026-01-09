from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.expect_all_or_drop({"valid_id":"company_id IS NOT NULL","valid_name":"company_name IS NOT NULL"})
@dp.table(name="silver_company")
def silver_company():
    df=spark.readStream.table("bronze_company")
    df_silver=df.dropDuplicates(["company_id"])
    return df_silver


@dp.expect_all_or_drop({"valid_emp_id":"employee_id IS NOT NULL","valid_company_id":"company_id IS NOT NULL"})
@dp.table(name="silver_employee")
def silver_employee():
    df=spark.readStream.table("bronze_employee")
    df_silver=df.dropDuplicates(["employee_id"]).withColumn("ssn_masked",concat(lit("xxx-xx-"),substring(col("ssn"),-4,4))).drop("ssn")
    return df_silver

@dp.expect_all_or_drop({"valid_enr_id":"enrollment_id IS NOT NULL","valid_emp_id":"employee_id IS NOT NULL","valid_policy_id":"policy_id IS NOT NULL"})
@dp.table(name="silver_enrollment")
def silver_enrollment():
    df=spark.readStream.table("bronze_enrollment")
    df_silver=df.dropDuplicates(["enrollment_id"])
    return df_silver


@dp.expect_all_or_drop({"valid_claim_id":"claim_id IS NOT NULL","valid_member_id":"member_id IS NOT NULL","valid_policy_id":"policy_id IS NOT NULL","valid_provider_name":"provider_name IS NOT NULL","claim_amount_positive": "claim_amount > 0","valid_status": "claim_status IN ('APPROVED','DENIED','PENDING')"})
@dp.table(name="silver_claim")
def silver_claim():
    df=spark.readStream.table("bronze_claim")
    df_silver=df.dropDuplicates(["claim_id"])
    return df_silver

@dp.expect_all_or_drop({"valid_emp_id":"employee_id IS NOT NULL","valid_dependent_id":"dependent_id IS NOT NULL"})
@dp.table(name="silver_dependent")
def silver_dependent():
    df=spark.readStream.table("bronze_dependent")
    df_silver=df.dropDuplicates(["dependent_id"])
    return df_silver

@dp.expect_all_or_drop({"valid_plan_id":"plan_id IS NOT NULL","valid_plan_name":"plan_name IS NOT NULL"})
@dp.table(name="silver_plan")
def silver_plan():
    df=spark.readStream.table("bronze_plan")
    df_silver=df.dropDuplicates(["plan_id"])
    return df_silver

@dp.expect_all_or_drop({"valid_plan_id":"plan_id IS NOT NULL","valid_policy_number":"policy_number IS NOT NULL","valid_policy_id":"policy_id IS NOT NULL"})
@dp.table(name="silver_policy")
def silver_policy():
    df=spark.readStream.table("bronze_policy")
    df_silver=df.dropDuplicates(["policy_id"])
    return df_silver

@dp.expect_all_or_drop({"valid_payment_id":"payment_id IS NOT NULL","valid_policy_id":"policy_id IS NOT NULL"})
@dp.table(name="silver_payment")
def silver_payment():
    df=spark.readStream.table("bronze_payment")
    df_silver=df.dropDuplicates(["payment_id"])
    return df_silver
