--creating view for customer

create view gold.dim_customers as 
select
  row_number() over(order by cst_id) as customer_key,
 ci.cst_id as customer_id,
 ci.cst_key as customer_number,
 ci.cst_firstname as first_name,
 ci.cst_lastname as last_name,
 la.cntry as country,
 ci.cst_marital_status as marital_status,
 
 case when ci.cst_gndr != 'N/A' then ci.cst_gndr
      else coalesce(ca.gen, 'N/A')
  end as gender,  
  ca.bdate as birthdate,
  ci.cst_create_date as create_date,
  
  from DATAWAREHOUSE.SILVER.CRM_CUST_INFO ci
left outer join DATAWAREHOUSE.SILVER.ERP_CUST_AZ12 ca
on ci.cst_key = ca.cid
left outer join DATAWAREHOUSE.SILVER.ERP_LOC_A101 la
on ci.cst_key = la.cid;

select distinct gender from DATAWAREHOUSE.GOLD.DIM_CUSTOMERS;

--creating view for product

create or replace view gold.dim_product as
select 
  row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pn.prd_cost as cost,
pn.prd_line as prodict_line,
pn.prd_start_dt as start_date,
pc.maintenance
from DATAWAREHOUSE.SILVER.CRM_PRD_INFO pn
left outer join DATAWAREHOUSE.SILVER.ERP_PX_CAT_G1V2 pc
on pn.cat_id = pc.id
where prd_end_dt is null; --- filter out all historical data

select * from DATAWAREHOUSE.GOLD.DIM_PRODUCT;

--Fact Table creation

create or replace view gold.fact_sales as
select
  sd.sls_ord_num as order_number,
  pr.product_key,
  cu.customer_key,
  sd.sls_order_dt as order_date,
  sd.sls_ship_dt as shipping_date,
  sd.sls_due_dt as due_date,
  sd.sls_sales as sales_amount,
  sd.sls_quantity as quantity,
  sd.sls_price as price
from DATAWAREHOUSE.SILVER.CRM_SALES_DETAILS sd
left outer join gold.dim_product pr
on sd.sls_prd_key = pr.product_number
left outer join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id;

select * from DATAWAREHOUSE.GOLD.FACT_SALES;

--Foreign key integrity

select * 
from DATAWAREHOUSE.GOLD.FACT_SALES f
left outer join DATAWAREHOUSE.GOLD.DIM_CUSTOMERS c
on c.customer_key = f.customer_key
left outer join DATAWAREHOUSE.GOLD.DIM_PRODUCT p
on p.product_key = f.product_key
where c.customer_key is null;
