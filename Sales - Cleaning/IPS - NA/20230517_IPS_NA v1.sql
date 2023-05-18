with
all_fields as(
SELECT DISTINCT 
ENTITY,date_add(date(concat(20,left(safe_cast(transaction_date as string),2),'-',right(safe_cast(transaction_date as string),2),'-','01')), interval -6 month) as TRANSACTION_DATE_f,transaction_date,CUSTOMER_ID, customer_name,Customer_Group_Master_Account as cust_group,SKU,Unit_of_Measurement,product__description,product__class,product_type,Primary_Substrate_,end_market,sum(Sales_Amount) as sales
, sum(safe_cast(quantity_f as float64)) as quantity_f
, sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
from(select *
,case when quantity like '%(%' then 1 else 0 end as check
,case when quantity like '%(%' then concat('-',right(left(quantity,length(quantity)-1),length(quantity)-3))
when quantity='-' then '0' 
when quantity like '%.%' then replace(quantity,'.','')
else quantity end as quantity_f
FROM `responsive-gist-387019.tekniplex.20230517_FY2023_114_a_134` )
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
--ORDER BY 1
)
,nulls_fixed as(
select distinct entity,transaction_date,sku,product__description,product__class,product_type,primary_substrate_,end_market,customer_id,cust_group,customer_name
--En su mayoria esta rellenando skus agrupados por ej rebates, discounts
/*,case when product__description is null then first_value(product__description) over(partition by sku order by product__description desc) else product__description end as product_Description_fix
,case when product__class is null then first_value(product__class) over(partition by sku order by product__class desc) else product__class end as fix_product__class
,case when product_type is null then first_value(product_type) over(partition by sku order by product_type desc) else product_type end as fix_product_type
,case when primary_substrate_ is null then first_value(primary_substrate_) over(partition by sku order by primary_substrate_ desc) else primary_substrate_ end as fix_primary_substrate_
,case when end_market is null then first_value(end_market) over(partition by sku order by end_market desc) else end_market end as fix_end_market*/
--
,case when cust_group is null then first_value(cust_group) over(partition by customer_id order by cust_group desc) else cust_group end as fix_cust_group
,case when customer_name is null then first_value(customer_name) over(partition by customer_id order by customer_name desc) else customer_name end as fix_customer_name
,sum(Sales) as sales
, sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
from all_fields
group by 1,2,3,4,5,6,7,8,9,10,11
)



select distinct product__class
from nulls_fixed
--limit 10
