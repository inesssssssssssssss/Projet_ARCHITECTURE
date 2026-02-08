--Création de la base 'ANYCOMPANY_LAB'

create or replace database ANYCOMPANY_LAB;

-- Crée ou remplace le schéma 'BRONZE ' dans la base 'ANYCOMPANY_LAB'

create or replace schema BRONZE ;

-- Crée ou remplace le schéma 'SILVER' dans la base 'ANYCOMPANY_LAB'

create or replace schema SILVER;

use schema BRONZE;

-- Stage public S3 

create or replace stage Amazon_S3
  url='s3://logbrain-datalake/datasets/food-beverage/';
  
list @Amazon_S3;
--creation fichier CSV
create or replace file format csv
  type = 'CSV'
  field_delimiter = ','
  record_delimiter = '\n'
  skip_header = 1
  field_optionally_enclosed_by = '\042'
  null_if = ('');
  
-- Creation fichier json
create or replace file format json
  type = 'JSON'
  strip_outer_array = true;

  --CREATION DES TABLES brutes
-- customer_demographics
CREATE OR REPLACE TABLE CUSTOMER_DEMOGRAPHICS (
    CUSTOMER_ID         VARCHAR,
    NAME                VARCHAR,
    DATE_OF_BIRTH       VARCHAR,
    GENDER              VARCHAR,
    REGION              VARCHAR,
    COUNTRY             VARCHAR,
    CITY                VARCHAR,
    MARITAL_STATUS      VARCHAR,
    ANNUAL_INCOME       VARCHAR
);

--customer_service_interactions
CREATE OR REPLACE TABLE CUSTOMER_SERVICE_INTERACTIONS (
    INTERACTION_ID          VARCHAR,
    INTERACTION_DATE        VARCHAR,
    INTERACTION_TYPE        VARCHAR,
    ISSUE_CATEGORY          VARCHAR,
    DESCRIPTION             VARCHAR,
    DURATION_MINUTES        VARCHAR,
    RESOLUTION_STATUS       VARCHAR,
    FOLLOW_UP_REQUIRED      VARCHAR,
    CUSTOMER_SATISFACTION   VARCHAR
);

--financial_transactions

CREATE OR REPLACE TABLE FINANCIAL_TRANSACTIONS (
    TRANSACTION_ID      VARCHAR,
    TRANSACTION_DATE    VARCHAR,
    TRANSACTION_TYPE    VARCHAR,
    AMOUNT              VARCHAR,
    PAYMENT_METHOD      VARCHAR,
    ENTITY              VARCHAR,
    REGION              VARCHAR,
    ACCOUNT_CODE        VARCHAR
);

--promotions_data
CREATE OR REPLACE TABLE PROMOTIONS_DATA (
    PROMOTION_ID            VARCHAR,
    PRODUCT_CATEGORY        VARCHAR,
    PROMOTION_TYPE          VARCHAR,
    DISCOUNT_PERCENTAGE     VARCHAR,
    START_DATE              VARCHAR,
    END_DATE                VARCHAR,
    REGION                  VARCHAR
);

--marketing_campaigns
CREATE OR REPLACE TABLE MARKETING_CAMPAIGNS (
    CAMPAIGN_ID         VARCHAR,
    CAMPAIGN_NAME       VARCHAR,
    CAMPAIGN_TYPE       VARCHAR,
    PRODUCT_CATEGORY    VARCHAR,
    TARGET_AUDIENCE     VARCHAR,
    START_DATE          VARCHAR,
    END_DATE            VARCHAR,
    REGION              VARCHAR,
    BUDGET              VARCHAR,
    REACH               VARCHAR,
    CONVERSION_RATE     VARCHAR
);

--product_reviews

CREATE OR REPLACE TABLE product_reviews (
    review_id INT,
    product_id VARCHAR(50),
    reviewer_id VARCHAR(50),
    reviewer_name VARCHAR(100),
    helpful_votes INT,              
    total_votes INT,                
    rating INT,                     
    review_datetime TIMESTAMP,      
    review_title VARCHAR(500),      
    review_text VARCHAR(5000),      
    product_category_1 VARCHAR(200),
    product_category_2 VARCHAR(200),
    product_description VARCHAR(2000),
    empty_col VARCHAR(10)           
);

--logistics_and_shipping
CREATE OR REPLACE TABLE LOGISTICS_AND_SHIPPING (
    SHIPMENT_ID             VARCHAR,
    ORDER_ID                VARCHAR,
    SHIP_DATE               VARCHAR,
    ESTIMATED_DELIVERY      VARCHAR,
    SHIPPING_METHOD         VARCHAR,
    STATUS                  VARCHAR,
    SHIPPING_COST           VARCHAR,
    DESTINATION_REGION      VARCHAR,
    DESTINATION_COUNTRY     VARCHAR,
    CARRIER                 VARCHAR
);

--supplier_information
CREATE OR REPLACE TABLE SUPPLIER_INFORMATION (
    SUPPLIER_ID         VARCHAR,
    SUPPLIER_NAME       VARCHAR,
    PRODUCT_CATEGORY    VARCHAR,
    REGION              VARCHAR,
    COUNTRY             VARCHAR,
    CITY                VARCHAR,
    LEAD_TIME           VARCHAR,
    RELIABILITY_SCORE   VARCHAR,
    QUALITY_RATING      VARCHAR
);

--employee_records
CREATE OR REPLACE TABLE EMPLOYEE_RECORDS (
    EMPLOYEE_ID     VARCHAR,
    NAME            VARCHAR,
    DATE_OF_BIRTH   VARCHAR,
    HIRE_DATE       VARCHAR,
    DEPARTMENT      VARCHAR,
    JOB_TITLE       VARCHAR,
    SALARY          VARCHAR,
    REGION          VARCHAR,
    COUNTRY         VARCHAR,
    EMAIL           VARCHAR
);

--Création des variants Json

CREATE OR REPLACE TABLE JSON_INVENTORY_DATA (v VARIANT);
CREATE OR REPLACE TABLE JSON_STORE_LOCATIONS_DATA (v VARIANT);
