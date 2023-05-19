with
all_fields as(
SELECT DISTINCT entity, "Integrated Performance Solutions" as Business_Unit
,case when entity in(114,115) then 'Dispensing' else 'Sealing' end as Main_Business_Line,2023 as FY
,date_add(date(concat(20,left(safe_cast(transaction_date as string),2),'-',right(safe_cast(transaction_date as string),2),'-','01')), interval -6 month) as transaction_date,CUSTOMER_ID, customer_name,Customer_Group_Master_Account as cust_group,customer_location,SKU,Unit_of_Measurement,product__description,product__class,product_type,Primary_Substrate_,end_market
,case when entity in(114,115) then 'Dispensing' else 'Sealing' end as product_line
,sum(Sales_Amount) as sales
,sum(safe_cast(quantity_f as float64)) as quantity_f
,sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
,sum(safe_cast(Discount_Rebate_Amount as float64)) as discount,sum(safe_cast(Return_Tagging as float64)) as Return_Tagging
from(select *
,case when quantity like '%(%' then concat('-',right(left(quantity,length(quantity)-1),length(quantity)-3))
when quantity='-' then '0' 
when quantity like '%.%' then replace(quantity,'.','')
else quantity end as quantity_f
FROM `responsive-gist-387019.tekniplex.20230517_FY2023_114_a_134` )
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
--ORDER BY 1
)
,nulls_fixed as(
select distinct entity,business_unit,main_business_line,fy,transaction_date
,sku,product__description as sku_description,product__class,product_type,primary_substrate_,end_market
,customer_id,cust_group,customer_name,customer_location,product_line
--En su mayoria esta rellenando skus agrupados por ej rebates, discounts
/*,case when product__description is null then first_value(product__description) over(partition by sku order by product__description desc) else product__description end as product_Description_fix
,case when product__class is null then first_value(product__class) over(partition by sku order by product__class desc) else product__class end as fix_product__class
,case when product_type is null then first_value(product_type) over(partition by sku order by product_type desc) else product_type end as fix_product_type
,case when primary_substrate_ is null then first_value(primary_substrate_) over(partition by sku order by primary_substrate_ desc) else primary_substrate_ end as fix_primary_substrate_
,case when end_market is null then first_value(end_market) over(partition by sku order by end_market desc) else end_market end as fix_end_market*/
--
,case when cust_group is null then first_value(cust_group) over(partition by customer_id order by cust_group desc) else cust_group end as fix_cust_group
,case when customer_name is null then first_value(customer_name) over(partition by customer_id order by customer_name desc) else customer_name end as fix_customer_name
,Unit_of_Measurement
,sum(quantity_f) as quantity
,sum(Sales) as sales,sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
from all_fields
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
,Unit_of_Measurement
)
,repeated_skus as(
select distinct entity,sku,count(*) as rep
from nulls_fixed
group by 1,2
)
,check_standard as(
select distinct *
,case when round((standard_cost/nullif(quantity,0)),0)=p_sku then 1 else 0 end as check
from nulls_fixed a left join (
select distinct n.entity as entity_,n.sku as sku_,r.rep,round((sum(standard_cost)/nullif(sum(quantity),0)),0) as p_sku
from nulls_fixed n left join repeated_skus r on n.entity=r.entity and n.sku=r.sku
group by 1,2,3
--having rep>1 and p_sku>0
order by 1,2,3 desc) b on a.entity=b.entity_ and a.sku=b.sku_
order by a.entity, a.sku, transaction_date,check desc
)
/*--Prender para validar si es estandar o real
select distinct entity,check,count(distinct sku)
from check_standard
--where check=0
group by 1,2 order by 1,2
--*/
--Prender para validar units
/*
select distinct sku,unit_of_measurement,sum(quantity)
from nulls_fixed
group by 1,2 order by 1,2
*/

select distinct entity,business_unit,main_business_line,fy,transaction_date
,customer_id,customer_name,customer_location,cust_group
,sku,sku_description,product_line,product__class as product_class,product_type,Primary_Substrate_ as Primary_Substrate,end_market
,null as Product_Class_Homologated
,null as Product_Type_Homologated
,null as Primary_Substrate_Homologated
,null as End_Market_Homologated
,unit_of_measurement as un_of_measurement
,null as Intercompany_Transaction_Tagging
,sum(quantity) as quantity,sum(Sales) as sales,sum(Standard_Cost) as Costs
,sum(Standard_Material) as raw_material
,sum(Standard_Labor) as operating_cost_incl_dl
,sum(Standard_OH) as indirect_cost_incl_oh
,sum(sales) as sales_w_discount
,null as costs_pr
,null as raw_material_pr
,null as operating_cost_incl_dl_pr
,null as indirect_cost_incl_oh_pr
,null as discount_pr
,null as Return_Tagging
from nulls_fixed
--limit 10
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
order by 1--,2
