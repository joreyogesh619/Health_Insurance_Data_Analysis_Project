CREATE TABLE company (
    company_id STRING,
    company_name STRING,
    fein STRING,
    address STRING,
    state STRING,
    industry STRING,
    active_flag BOOLEAN,
    created_date DATE,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE employee (
    employee_id STRING,
    company_id STRING,
    ssn STRING,
    first_name STRING,
    last_name STRING,
    dob DATE,
    gender STRING,
    email STRING,
    phone STRING,
    hire_date DATE,
    employment_status STRING,
    created_date DATE,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE dependent (
    dependent_id STRING,
    employee_id STRING,
    first_name STRING,
    last_name STRING,
    relationship STRING,
    dob DATE,
    gender STRING,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE plan (
    plan_id STRING,
    plan_name STRING,
    plan_type STRING,
    deductible DECIMAL(10,2),
    out_of_pocket_max DECIMAL(10,2),
    copay DECIMAL(10,2),
    coinsurance_pct DECIMAL(5,2),
    coverage_level STRING,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE policy (
    policy_id STRING,
    policy_number STRING,
    plan_id STRING,
    policy_start_date DATE,
    policy_end_date DATE,
    policy_status STRING,
    created_date DATE,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE enrollment (
    enrollment_id STRING,
    employee_id STRING,
    policy_id STRING,
    enrollment_date DATE,
    termination_date DATE,
    coverage_type STRING,
    enrollment_status STRING,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE claim (
    claim_id STRING,
    policy_id STRING,
    member_id STRING, -- employee_id or dependent_id
    provider_name STRING,
    service_date DATE,
    claim_amount DECIMAL(12,2),
    approved_amount DECIMAL(12,2),
    claim_status STRING,
    processed_date DATE,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;

CREATE TABLE payment (
    payment_id STRING,
    policy_id STRING,
    payment_date DATE,
    amount DECIMAL(12,2),
    payment_method STRING,
    payment_status STRING,

    -- ETL Metadata
    source_system STRING,
    etl_batch_id STRING,
    created_ts TIMESTAMP,
    updated_ts TIMESTAMP,
    is_current_flag BOOLEAN
)
USING DELTA;