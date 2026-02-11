/*
================================================================================
Coventry Building Society - Data Engineer Case Study
Candidate: Ravindra Kumar
Environment: Databricks (Unity Catalog Enabled)
Catalog: banking_catalog
Schema: bronze
Storage: External Volume backed by AWS S3
Format: Delta Tables (converted from raw CSV)

================================================================================
BACKGROUND CONTEXT

The raw CSV files were stored in a Unity Catalog external volume:
  /Volumes/banking_catalog/bronze/raw_ingestion_volume/

These files were ingested into managed Delta tables within the bronze schema
using read_files() to ensure:
  - ACID compliance
  - Schema enforcement
  - Query performance
  - Compatibility with production Lakehouse architecture

All queries below are executed in:

  USE CATALOG banking_catalog;
  USE SCHEMA bronze;

================================================================================
TABLE OF CONTENTS

1. Basic Queries
   1a Total Balance per Customer & Account Type
   1b Accounts opened in last 365 days (Multi-Approach)
   1c Top 5 Customers by Total Balance

2. Transaction Analysis
   2a Large Withdrawals in last 30 days (Multi-Approach)
   2b Total Deposits in last 6 months (Multi-Approach)
   2c Running Balance (Window Function + Alternative)

3. Advanced Queries
   3a Average Transaction vs Balance (Multi-Approach)
   3b Most Frequent Customer

4. Data Quality Checks
   4a Balance Mismatch
   4b Missing Customer Data
   4c Duplicate Accounts
   4d Invalid Transaction Types
   4e Negative Non-Credit Accounts

================================================================================
*/

USE CATALOG banking_catalog;
USE SCHEMA bronze;


-- 1a) TOTAL BALANCE PER CUSTOMER & ACCOUNT TYPE


SELECT
    customer_id,
    account_type,
    SUM(balance) AS total_balance
FROM accounts
GROUP BY customer_id, account_type
ORDER BY customer_id, account_type;



-- 1b) ACCOUNTS OPENED IN LAST 365 DAYS
-- Multi-Approach

-- Approach 1: Using DATE_SUB (Databricks native)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    a.account_id,
    a.opening_date
FROM accounts a
JOIN customers c
    ON a.customer_id = c.customer_id
WHERE a.opening_date >= DATE_SUB(CURRENT_DATE, 365);


-- Approach 2: Using INTERVAL syntax

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    a.account_id,
    a.opening_date
FROM accounts a
JOIN customers c
    ON a.customer_id = c.customer_id
WHERE a.opening_date >= CURRENT_DATE - INTERVAL 365 DAYS;



-- 2a) WITHDRAWALS > 500 IN LAST 30 DAYS
-- Multi-Approach

-- Approach 1: Standard Filter

SELECT
    a.account_id,
    c.customer_id,
    c.first_name,
    c.last_name,
    t.transaction_date,
    t.amount
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
JOIN customers c ON a.customer_id = c.customer_id
WHERE t.transaction_type = 'Withdrawal'
  AND t.amount > 500
  AND t.transaction_date >= DATE_SUB(CURRENT_DATE, 30);


-- Approach 2: Using CTE for readability

WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE transaction_type = 'Withdrawal'
      AND amount > 500
      AND transaction_date >= DATE_SUB(CURRENT_DATE, 30)
)

SELECT
    a.account_id,
    c.customer_id,
    c.first_name,
    c.last_name,
    ft.transaction_date,
    ft.amount
FROM filtered_transactions ft
JOIN accounts a ON ft.account_id = a.account_id
JOIN customers c ON a.customer_id = c.customer_id;



-- 2b) TOTAL DEPOSITS IN LAST 6 MONTHS
-- Multi-Approach

-- Approach 1: ADD_MONTHS

SELECT
    a.customer_id,
    SUM(t.amount) AS total_deposits
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
WHERE t.transaction_type = 'Deposit'
  AND t.transaction_date >= ADD_MONTHS(CURRENT_DATE, -6)
GROUP BY a.customer_id;


-- Approach 2: Conditional Aggregation

SELECT
    a.customer_id,
    SUM(
        CASE
            WHEN t.transaction_type = 'Deposit'
             AND t.transaction_date >= ADD_MONTHS(CURRENT_DATE, -6)
            THEN t.amount
            ELSE 0
        END
    ) AS total_deposits
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
GROUP BY a.customer_id;



-- 2c) RUNNING BALANCE

-- Approach 1: Window Function (Recommended)

WITH txn_effects AS (
    SELECT
        account_id,
        transaction_id,
        transaction_date,
        transaction_type,
        amount,
        CASE
            WHEN transaction_type = 'Deposit' THEN amount
            WHEN transaction_type IN ('Withdrawal','Payment','Transfer') THEN -amount
            ELSE 0
        END AS net_amount
    FROM transactions
)

SELECT
    account_id,
    transaction_id,
    transaction_date,
    transaction_type,
    amount,
    SUM(net_amount) OVER (
        PARTITION BY account_id
        ORDER BY transaction_date, transaction_id
    ) AS running_balance
FROM txn_effects;



-- 3a) AVG TRANSACTION VS AVG BALANCE
-- Multi-Approach

-- Approach 1: CTE + Join

WITH yearly_txn AS (
    SELECT
        a.customer_id,
        t.amount
    FROM transactions t
    JOIN accounts a ON t.account_id = a.account_id
    WHERE t.transaction_date >= ADD_MONTHS(CURRENT_DATE, -12)
)

SELECT
    yt.customer_id,
    AVG(yt.amount) AS avg_transaction_amount,
    AVG(a.balance) AS avg_balance
FROM yearly_txn yt
JOIN accounts a ON yt.customer_id = a.customer_id
GROUP BY yt.customer_id;


-- Approach 2: Direct Aggregation

SELECT
    a.customer_id,
    AVG(t.amount) AS avg_transaction_amount,
    AVG(a.balance) AS avg_balance
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
WHERE t.transaction_date >= ADD_MONTHS(CURRENT_DATE, -12)
GROUP BY a.customer_id;



-- 4a) BALANCE MISMATCH

WITH calculated_balance AS (
    SELECT
        account_id,
        SUM(
            CASE
                WHEN transaction_type = 'Deposit' THEN amount
                WHEN transaction_type IN ('Withdrawal','Payment','Transfer') THEN -amount
                ELSE 0
            END
        ) AS calculated_balance
    FROM transactions
    GROUP BY account_id
)

SELECT
    a.account_id,
    a.balance AS account_balance,
    cb.calculated_balance
FROM accounts a
LEFT JOIN calculated_balance cb
    ON a.account_id = cb.account_id
WHERE a.balance <> cb.calculated_balance;



-- 4b) MISSING CUSTOMER INFORMATION

SELECT
    customer_id,
    first_name,
    last_name,
    CONCAT_WS(', ',
        CASE WHEN address IS NULL THEN 'Missing Address' END,
        CASE WHEN date_of_birth IS NULL THEN 'Missing DOB' END,
        CASE WHEN zip IS NULL THEN 'Missing ZIP' END
    ) AS missing_fields
FROM customers
WHERE address IS NULL
   OR date_of_birth IS NULL
   OR zip IS NULL;



-- 4c) DUPLICATE ACCOUNT TYPES (same type for same customer)

SELECT
    customer_id,
    account_type,
    COUNT(*) AS number_of_duplicates
FROM accounts
GROUP BY customer_id, account_type
HAVING COUNT(*) > 1;



-- 4d) INVALID TRANSACTION TYPES (anything outside expected list)

SELECT
    transaction_id,
    account_id,
    transaction_type
FROM transactions
WHERE transaction_type NOT IN ('Deposit','Withdrawal','Payment','Transfer');



-- 4e) NEGATIVE NON-CREDIT ACCOUNTS

SELECT
    account_id,
    customer_id,
    account_type,
    balance
FROM accounts
WHERE account_type <> 'Credit'
  AND balance < 0;
