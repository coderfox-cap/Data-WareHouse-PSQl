
--THIS FILE IS USED TO CHECK FOR THE ANOMALIES IN THE TABLES 

--FIRST WE  CHECK crm_cust_info table 
select * from bronze.crm_cust_info where cst_id=29466

select *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
from bronze.crm_cust_info where cst_id=29466 -- the purpose of this query is to rank the recent data

select *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
from bronze.crm_cust_info -- here it flags all the recent records as 1 so we can extract the newest data
select * from (
select *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last 
from bronze.crm_cust_info ) where flag_last = 1;-- this fetch all the unique 

--checnking for unncessary spaces in first and last name
select cst_lastname from (
select length(cst_lastname) as len1, length(trim(cst_lastname)) as len2,cst_lastname 
from bronze.crm_cust_info) where len1 != len2

SELECT DISTINCT cst_gndr from bronze.crm_cust_info;

SELECT DISTINCT cst_marital_status from bronze.crm_cust_info;
SELECT cst_firstname 
from silver.crm_cust_info where cst_firstname != TRIM(cst_firstname)

--completely cleanded sql code
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

--NOW WE WILL explore the crm_prd_info table 
SELECT
	prd_id,
	prd_key,
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
	CAST(prd_start_dt AS DATE),
		CAST(LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt) - INTERVAL '1 DAY' AS DATE) AS prd_end_dt
	FROM bronze.crm_prd_info

--check for duplicates in PRIMARY KEY
select prd_id,count(*) from bronze.crm_prd_info 
group by prd_id  having count(*) >1 --RESULT NO NULLS / DUPLICATES

select distinct  id from bronze.erp_px_cat_g1v2--comparing the cat_id with ID 
--check for unwanted spaces
select prd_nm from bronze.crm_prd_info where prd_nm != TRIM(prd_nm) --Result there is no Unwanted spaces

--we will check for cost wheather it has negative or null values
select prd_cost from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null

select  distinct prd_line from bronze.crm_prd_info;

--now checking for dates like end date come before the start date which is wrong
select
	prd_id,
	prd_key,
	prd_nm,
	prd_start_dt,
	prd_end_dt,
	LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt) - INTERVAL '1 DAY' AS prd_end_dt_test
from bronze.crm_prd_info 
WHERE prd_key in ('AC-HE-HL-U509-R','AC-HE-HL-U509')

--NOW WE WILL EXPLORE THE cem_sales_details table 

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

--check for unwanted spaces in sls_ord_num
select  * from bronze.crm_sales_details
	where sls_ord_num != trim(sls_ord_num) 
--coverting the date to right format like int to date in (sls_order_dt,sls_due_dt,sls_ship_dt)
select NULLIF(sls_order_dt,0) from bronze.crm_sales_details where sls_order_dt <= 0; -- we have 0 in date column which is bad
--converting the int to date 
SELECT
CASE WHEN sls_order_dt <=0 OR LENGTH(CAST(sls_order_dt AS VARCHAR)) !=8 THEN NULL
   ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
END  sls_order_date
FROM bronze.crm_sales_details;

select sls_sales  AS old_sales, sls_quantity  , sls_price as old_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
	THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END sls_sales,
CASE WHEN sls_price IS NULL OR sls_price <=0
	THEN sls_sales / NULLIF(sls_quantity,0)
    ELSE sls_price
END  sls_price
from bronze.crm_sales_details
WHERE sls_sales * sls_quantity != sls_price
OR sls_sales IS NULL  OR sls_quantity IS NULL OR  sls_price IS NULL  
OR sls_sales <=0  OR sls_quantity <=0 OR  sls_price <=0  ;

select * from bronze.crm_sales_details;

--NOW WE WILL EXPLORE THE erp_cust_az12 tabel
select * from bronze.erp_cust_az12

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

SELECT bdate from bronze.erp_cust_az12 WHERE bdate < '1925-01-01' OR bdate > NOW() 
ORDER BY bdate DESC -- bad data as the customer lives more than 100 years or yet to be bron in future

SELECT 
CASE WHEN gen ='' THEN NULL
	WHEN gen ='F' THEN 'Female'
	WHEN gen ='M' THEN 'Male'
	ELSE gen
END as gen,
from bronze.erp_cust_az12 
--NOW WE WILL EXPLORE erp_loc_a101

select REPLACE(cid,'-',''),
CASE WHEN cntry ='DE' THEN 'Germany'
	WHEN cntry in ('US','USA') THEN 'United States'
	WHEN cntry='' OR cntry IS NULL THEN 'N/A'
	ELSE TRIM(cntry)
END cntry
from bronze.erp_loc_a101
--now we will explore erp_px_cat_g1v2
SELECT
	id,
	cat,
	subcat,
	maintenance
	from bronze.erp_px_cat_g1v2
