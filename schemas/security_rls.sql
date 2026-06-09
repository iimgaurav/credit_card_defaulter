-- RLS + Column Masking for Gold Layer
-- Run in Databricks SQL Editor or via notebooks/06_monitoring/sql_alerts.py
--
-- Prerequisites:
--   Catalog: credit_card_dev
--   Roles/groups configured in Unity Catalog
--   Tables already exist in gold schema

-- ── 1. Column Masking: PII on dim_customer ──────────────────────────────────

-- Mask email (show first 3 chars + domain)
CREATE OR REPLACE FUNCTION gold.mask_email(email STRING)
RETURN IF(is_account_group_member('data_engineers') OR is_account_group_member('admins'),
           email,
           CONCAT(LEFT(email, 3), '****@', SPLIT(email, '@')[1]));

ALTER TABLE gold.dim_customer ALTER COLUMN email SET MASK gold.mask_email;

-- Mask phone (show last 4 digits)
CREATE OR REPLACE FUNCTION gold.mask_phone(phone STRING)
RETURN IF(is_account_group_member('data_engineers') OR is_account_group_member('admins'),
           phone,
           CONCAT('***-***-', RIGHT(phone, 4)));

ALTER TABLE gold.dim_customer ALTER COLUMN phone_number SET MASK gold.mask_phone;

-- Mask full_name (show first name + last initial)
CREATE OR REPLACE FUNCTION gold.mask_name(name STRING)
RETURN IF(is_account_group_member('data_engineers') OR is_account_group_member('admins'),
           name,
           CONCAT(SPLIT(name, ' ')[0], ' ', LEFT(SPLIT(name, ' ')[1], 1), '.'));

ALTER TABLE gold.dim_customer ALTER COLUMN full_name SET MASK gold.mask_name;

-- ── 2. Row-Level Security: Region-Based Access ───────────────────────────────

-- Row filter: restrict to user's state_code (maps UPN to region)
CREATE OR REPLACE ROW FILTER gold.customer_region_filter
ON gold.dim_customer
AS () -> is_account_group_member('admins')
         OR state_code IN (
             SELECT state_code FROM gold.dim_geography
             WHERE region = COALESCE(
                 (SELECT region FROM gold.user_regions WHERE user_email = CURRENT_USER()),
                 'NONE'
             )
         );

ALTER TABLE gold.dim_customer SET ROW FILTER gold.customer_region_filter;

-- Row filter: restrict fact_transaction to customer's region
CREATE OR REPLACE ROW FILTER gold.fact_region_filter
ON gold.fact_transaction
AS () -> is_account_group_member('admins')
         OR customer_sk IN (
             SELECT customer_sk FROM gold.dim_customer
             WHERE state_code IN (
                 SELECT state_code FROM gold.dim_geography
                 WHERE region = COALESCE(
                     (SELECT region FROM gold.user_regions WHERE user_email = CURRENT_USER()),
                     'NONE'
                 )
             )
         );

ALTER TABLE gold.fact_transaction SET ROW FILTER gold.fact_region_filter;

-- ── 3. Staging Table: User → Region Mapping ──────────────────────────────────

CREATE TABLE IF NOT EXISTS gold.user_regions (
    user_email STRING COMMENT 'User email / UPN',
    region     STRING COMMENT 'AWS region (us-east-1, eu-west-1, etc)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert example mappings (replace with actual directory integration)
INSERT INTO gold.user_regions VALUES
    ('analyst-east@bank.com',  'us-east-1',  CURRENT_TIMESTAMP()),
    ('analyst-west@bank.com',  'eu-west-1',  CURRENT_TIMESTAMP()),
    ('admin@bank.com',          'us-east-1',  CURRENT_TIMESTAMP());

-- ── 4. Verify Setup ──────────────────────────────────────────────────────────

-- As admin:
--   SELECT full_name, email, phone_number, state_code FROM gold.dim_customer LIMIT 5;
--   → sees unmasked values

-- As analyst-east (member of 'analysts', not 'data_engineers'):
--   SELECT full_name, email, phone_number, state_code FROM gold.dim_customer LIMIT 5;
--   → sees masked values, only rows for state_code in us-east-1
