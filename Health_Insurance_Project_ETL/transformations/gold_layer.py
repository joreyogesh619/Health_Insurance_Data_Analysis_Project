from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(name="dim_employee")
def dim_employee():
    df=spark.read.table("silver_employee")
    df_emp=df.select(
            "employee_id",
            "company_id",
            "first_name",
            "last_name",
            "gender",
            "dob",
            "email",
            "phone",
            "employment_status",
            "hire_date"
    )
    return df_emp


@dp.materialized_view(name="dim_company")
def dim_company():
    df=spark.read.table("silver_company")
    df_comp=df.select(
            "company_id",
            "company_name",
            "fein",
            "address",
            "state",
            "industry",
            "active_flag",
            "created_date"
    )
    return df_comp

@dp.materialized_view(name="fact_enrollment")
def fact_enrollment():
    df=spark.read.table("silver_enrollment")
    df_enroll=df.select(
            "enrollment_id",
            "employee_id",
            "policy_id",
            "enrollment_date",
            "termination_date",
            "coverage_type",
            "enrollment_status"
    )
    return df_enroll

@dp.materialized_view(
        name="gold_company_enrollment_stats",
        comment="Enrollment statistics per company"
)
def gold_company_enrollment_stats():
    df_emp=spark.read.table("silver_employee")
    df_enroll=spark.read.table("silver_enrollment")
    df_enroll_stats=df_enroll.join(df_emp,"employee_id").groupBy("company_id").agg(
            countDistinct("employee_id").alias("total_employees_enrolled"),
            countDistinct("policy_id").alias("total_policies"),
            sum(when(col("coverage_type")=="FAMILY",1).otherwise(0)).alias("family_coverage_count"),
            sum(when(col("coverage_type")=="INDIVIDUAL",1).otherwise(0)).alias("individual_coverage_count")
            )
    return df_enroll_stats

@dp.materialized_view(
        name="gold_plan_utilization_stats",
        comment="Plan utilization and cost metrics"
)
def gold_plan_utilization_stats():
    df_policy=spark.read.table("silver_policy")
    df_claim=spark.read.table("silver_claim")
    df_plan_stats=df_policy.join(df_claim,"policy_id","left").groupBy("policy_id").agg(
            countDistinct("policy_id").alias("active_policies"),
            count("claim_id").alias("total_claims"),
            sum("claim_amount").alias("total_claim_amount"),
            sum("approved_amount").alias("total_approved_amount"),
            avg("approved_amount").alias("avg_claim_cost")
            )
    return df_plan_stats

    @dp.materialized_view(
            name="gold_claims_performance",
        comment="Claims approval and denial statistics"
    )
    def gold_claims_performance():
        df_claim=spark.read.table("silver_claim")
        df_claim_stats=df_claim.groupBy("claim_status").agg(
                count("claim_id").alias("total_claims"),
                sum("claim_amount").alias("total_claim_amount"),
                sum("approved_amount").alias("total_approved_amount"),
                avg(datediff("processed_date", "service_date"))
                .alias("avg_processing_days"))
        return df_claim_stats


@dp.materialized_view(
        name="gold_monthly_claims_trend",
        comment="Monthly claim volume and cost trend"
)
def gold_monthly_claims_trend():
    df_claim=spark.read.table("silver_claim")
    df_claim.withColumn("claim_month",date_format("service_date","yyyy-MM")).groupBy("claim_month").agg(
                   count("claim_id").alias("total_claims"),
                   sum("claim_amount").alias("total_claim_amount"),
                   sum("approved_amount").alias("total_approved_amount")
    )
    df_claim_final=df_claim.drop("_START_AT","_END_AT")
    return df_claim_final

@dp.materialized_view(
        name="gold_policy_loss_ratio",
        comment="Premium vs claims loss ratio"
)
def gold_policy_loss_ratio():
        df_premium=spark.read.table("silver_payment")
        df_premium_stats=df_premium.groupBy("policy_id").agg(
                sum("amount").alias("total_premium")
        )
        df_claim=spark.read.table("silver_claim")
        df_claim_stats=df_claim.groupBy("policy_id").agg(
                sum("approved_amount").alias("total_claim_amount")
        )
        df_loss_ratio=df_premium_stats.join(df_claim_stats,"policy_id","left").withColumn("loss_ratio",col("total_claim_amount")/col("total_premium"))
        return df_loss_ratio

@dp.materialized_view(
        name="gold_high_cost_members",
        comment="Members with high claim costs"
)
def gold_high_cost_members():
        df_claim=spark.read.table("silver_claim")
        df_claim_stats=df_claim.groupBy("member_id").agg(
                sum("approved_amount").alias("total_claim_cost"),
                count("claim_id").alias("claim_count")
        ).filter(col("total_claim_cost")>10000)
        return df_claim_stats

@dp.materialized_view(
        name="gold_company_risk_profile",
        comment="Risk profile per company"
)
def gold_company_risk_profile():
        df_claim=spark.read.table("silver_claim")
        df_policy=spark.read.table("silver_policy")
        df_enroll=spark.read.table("silver_enrollment")
        df_employee=spark.read.table("silver_employee")
        
        df_claim_stats=df_claim.join(df_policy,"policy_id").\
                join(df_enroll,"policy_id").\
                join(df_employee,"employee_id").\
                groupBy("company_id").agg(
                        sum("approved_amount").alias("total_claim_cost"),
                        countDistinct("employee_id").alias("covered_lives"),
                        avg("approved_amount").alias("avg_claim_cost")
                )
        return df_claim_stats

@dp.materialized_view(
        name="gold_kpi_summary",
        comment="Executive-level KPIs"
)
def gold_kpi_summary():
        df_claim=spark.read.table("silver_claim")
        df_kpi=df_claim.agg(
                countDistinct("policy_id").alias("total_active_policies"),
                count("claim_id").alias("total_claims"),
                sum("approved_amount").alias("total_claims_paid"),
                avg("approved_amount").alias("avg_claim_amount") 
        )
        return df_kpi     