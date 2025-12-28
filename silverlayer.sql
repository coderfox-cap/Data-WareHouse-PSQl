CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE,
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP,
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);

TRUNCATE  TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)
SELECT 
	cst_id,
    cst_key,
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
   	CASE WHEN UPPER(TRIM( cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM( cst_marital_status))='M' THEN 'Married'
		ELSE 'N/A'
	END  cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
		ELSE 'N/A'
	END cst_gndr,
    cst_create_date
FROM (
	select *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
from bronze.crm_cust_info WHERE cst_id IS NOT NULL ) t  where flag_last = 1


SELECT * FROM silver.crm_cust_info LIMIT 30;

DROP TABLE silver.crm_prd_info;--SOME CHANGES ARE MADE IN TABLE SCHEMA
CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
	cat_id 		 VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);

INSERT INTO silver.crm_prd_info( -- INSERTING A CLEAN VERSION OF THE CRM_PRD_INFO INTO SILVER LAYER
	prd_id,
	cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,--Extracting the CAtegory id from product_key
	SUBSTRING(prd_key,7,LENGTH(prd_key)) AS prd_key,--EXTRACTING THE product key 
	prd_nm,
	COALESCE(prd_cost,0) AS pr_cost,--Replacing the null with 0
	CASE WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
		Else 'N/A'
	END prd_line,
	CAST(prd_start_dt AS DATE) AS prd_start_dt ,
		CAST(LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt) - INTERVAL '1 DAY' AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info

SELECT * FROM silver.crm_prd_info;

DROP TABLE silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	dwh_create_date    TIMESTAMP DEFAULT NOW()
);
INSERT INTO silver.crm_sales_details(
	sls_ord_num  ,
    sls_prd_key  ,
    sls_cust_id  ,
    sls_order_dt ,
    sls_ship_dt  ,
    sls_due_dt   ,
    sls_sales    ,
    sls_quantity ,
    sls_price   
)
select  
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE WHEN sls_order_dt <=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8 THEN NULL
        ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
    END  sls_order_dt,
	CASE WHEN sls_due_dt <=0 OR LENGTH(CAST(sls_due_dt AS VARCHAR)) !=8 THEN NULL
        ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
    END  sls_due_dt,
	CASE WHEN sls_ship_dt <=0 OR LENGTH(CAST(sls_ship_dt AS VARCHAR)) !=8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
    END  sls_ship_dt,
	CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
    END sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <=0
	THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
    END  sls_price
	from bronze.crm_sales_details;

select * from silver.crm_sales_details;
TRUNCATE TABLE silver.erp_cust_az12
INSERT INTO silver.erp_cust_az12 (
	cid,
	bdate,
	gen
)
select 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LENGTH(cid))
	ELSE cid
END AS cid,
CASE WHEN bdate > NOW() THEN NULL
ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) in ('F','FEMALE') THEN 'Female'
	WHEN UPPER(TRIM(gen)) in ('M','MALE') THEN 'Male'
	ELSE 'N/A'
END as gen
from bronze.erp_cust_az12 
TRUNCATE TABLE silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101 (
	cid,cntry
)
select REPLACE(cid,'-',''),
CASE WHEN cntry ='DE' THEN 'Germany'
	WHEN cntry in ('US','USA') THEN 'United States'
	WHEN cntry='' OR cntry IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END cntry
from bronze.erp_loc_a101
SELECT * FROM silver.erp_loc_a101
TRUNCATE TABLE silver.erp_px_cat_g1v2 
INSERT INTO silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
SELECT
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
SELECT * FROM silver.erp_px_cat_g1v2

SELECT 
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	px.cat AS category,
	px.subcat AS subcategory,
	px.maintenance ,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line, 
	pn.prd_start_dt AS start_date
	from silver.crm_prd_info as pn
	LEFT JOIN silver.erp_px_cat_g1v2 as px
	ON pn.cat_id = px.id
	WHERE prd_end_dt IS NULL; -- this gets the current data as it has start date but not end which means it is on going one

SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
	FROM silver.crm_sales_details as sd
	LEFT JOIN gold.dim_products as pr
	ON sd.sls_prd_key = pr.product_number
	LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id

