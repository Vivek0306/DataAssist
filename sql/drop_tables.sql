-- retail_raw
DROP TABLE IF EXISTS retail_raw.lineitem;
DROP TABLE IF EXISTS retail_raw.partsupp;
DROP TABLE IF EXISTS retail_raw.orders;
DROP TABLE IF EXISTS retail_raw.customer;
DROP TABLE IF EXISTS retail_raw.part;
DROP TABLE IF EXISTS retail_raw.supplier;
DROP TABLE IF EXISTS retail_raw.nation;
DROP TABLE IF EXISTS retail_raw.region;

-- retail_staging
DROP TABLE IF EXISTS retail_staging.lineitem;
DROP TABLE IF EXISTS retail_staging.partsupp;
DROP TABLE IF EXISTS retail_staging.orders;
DROP TABLE IF EXISTS retail_staging.customer;
DROP TABLE IF EXISTS retail_staging.part;
DROP TABLE IF EXISTS retail_staging.supplier;
DROP TABLE IF EXISTS retail_staging.nation;
DROP TABLE IF EXISTS retail_staging.region;
DROP TABLE IF EXISTS retail_staging.pipeline_watermarks;