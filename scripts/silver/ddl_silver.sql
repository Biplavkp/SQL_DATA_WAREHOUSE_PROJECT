truncate table DATAWAREHOUSE.SILVER.CRM_CUST_INFO;
insert into DATAWAREHOUSE.SILVER.CRM_CUST_INFO( --- inserted data into Silver cust_info table from bronze cust_info
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,

case when upper(trim(cst_marital_status)) = 'S' then 'Single'
      when upper(trim(cst_marital_status)) = 'M' then 'Married'
      else 'N/A'
  end as st_marital_status,

 case when upper(trim(cst_gndr)) = 'F' then 'Female'
      when upper(trim(cst_gndr)) = 'M' then 'Male'
      else 'N/A'
  end as cst_gndr,
  
cst_create_date
from (
select * ,
row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
from DATAWAREHOUSE.BRONZE.CRM_CUST_INFO
where cst_id is not null
)t
where flag_last = 1
;

truncate table DATAWAREHOUSE.SILVER.CRM_PRD_INFO;
insert into DATAWAREHOUSE.SILVER.CRM_PRD_INFO(
    prd_id,  
    cat_id,
    prd_key,
    prd_nm, 
    prd_cost,
    prd_line,
    prd_start_dt, 
    prd_end_dt 
)
select
prd_id,
replace(substr(prd_key,1,5), '-', '_') as cat_id,
substr(prd_key, 7, len(prd_key)) as prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,

case when upper(trim(prd_line)) = 'M' then 'Mountain'
     when upper(trim(prd_line)) = 'R' then 'Road'
     when upper(trim(prd_line)) = 'S' then 'Other Sales'
     when upper(trim(prd_line)) = 'T' then 'Touring'
     else 'N/A'
end as prd_line, 

cast(prd_start_dt as DATE) as prd_start_dt,
cast(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))as date) AS prd_end_dt
from DATAWAREHOUSE.BRONZE.CRM_PRD_INFO;


truncate table DATAWAREHOUSE.SILVER.CRM_SALES_DETAILS;
insert into DATAWAREHOUSE.SILVER.CRM_SALES_DETAILS(
    sls_ord_num ,
    sls_prd_key ,
    sls_cust_id ,
    sls_order_dt ,
    sls_ship_dt ,
    sls_due_dt ,
    sls_sales ,
    sls_quantity ,
    sls_price
 )
 select
sls_ord_num,
sls_prd_key,
sls_cust_id,

case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
     else date(sls_order_dt)
end as sls_order_dt ,

case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
     else date(sls_ship_dt)
end as sls_ship_dt ,

case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
     else date(sls_due_dt)
end as sls_due_dt ,

case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
     then sls_quantity * abs(sls_price)
   else sls_sales
end as sls_sales,

round(case when sls_price is null or sls_price <= 0
    then sls_sales / round(nullif(sls_quantity,0),2)
    else sls_price
end,2) as sls_price,

sls_quantity,
from DATAWAREHOUSE.BRONZE.CRM_SALES_DETAILS;

truncate table DATAWAREHOUSE.SILVER.ERP_CUST_AZ12;
insert into DATAWAREHOUSE.SILVER.ERP_CUST_AZ12(
cid, bdate, gen
)
select

case when cid like 'NAS%' then substr(cid,4,len(cid))
    else cid
end as cid,

case when bdate > current_date then null
     else bdate
end as bdate ,    

case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
     when upper(trim(gen)) in ('M', 'MALE') then 'Male'
     else 'N/A'
 end as gen 
from DATAWAREHOUSE.BRONZE.ERP_CUST_AZ12;

truncate table silver.erp_loc_a101;
insert into silver.erp_loc_a101(
cid, cntry
)
select
replace(cid, '-','') as cid,
case when trim(cntry) = 'DE' then 'Germany'
     when trim(cntry) in ('US','USA') then 'United States'
     when trim(cntry) = '' or cntry is null then 'N/A'
    else trim(cntry)
 end  as cntry   
from DATAWAREHOUSE.BRONZE.ERP_LOC_A101;

truncate table DATAWAREHOUSE.SILVER.ERP_PX_CAT_G1V2;
insert into DATAWAREHOUSE.SILVER.ERP_PX_CAT_G1V2(
id, cat, subcat, maintenance
)
select
id,
cat,
subcat,
maintenance
from DATAWAREHOUSE.BRONZE.ERP_PX_CAT_G1V2;
