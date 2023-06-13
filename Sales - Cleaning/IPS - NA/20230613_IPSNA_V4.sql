with all_fields_fix as(
select distinct entity,Business_Unit,Plant_Business_line
,date(concat(year,'-',lpad(safe_cast(month as string),2,'0'),'-','01')) as transaction_date, fy
,CUSTOMER_ID, customer_name,customer_location,Customer_GroupMaster_Account as cust_group
,SKU,description as sku_description,_product_line_ as product_line,product_class,product_type,Primary_Substrate_ as primary_substrate,end_market
,_Product_Class_Homologated_ as Product_Class_Homologated,_Product_Type_Homologated_ as Product_Type_Homologated,_Primary_Substrate_Homologated_ as Primary_Substrate_Homologated,_End_Market_Homologated_ as End_Market_Homologated,NegocioIntercompany_Transaction_Tagging,Un_of_Measurement
,sum(quantity) as quantity,sum(safe_cast(replace(Sum_of_Units____,",",".") as float64)) as sum_of_units,sum(safe_cast(replace(Kg_sales,",",".") as float64)) as kg_sales
,round(sum(_sales_USD_/100),2) as sales
,round(sum(_Sum_of_costs_/100),2) as costs
,round(sum(_Raw_Material_USD_/100),2) as raw_material
,round(sum(_Operating_Cost_including_Direct_Labor_USD_/100),2) as operating_cost_incl_dl 	 
,round(sum(_Indirect_Cost_including_OH_USD_/100),2) as indirect_cost_incl_oh
,round(sum(_Sales_USD_wdiscount_/100),2) as sales_w_discount
,round(sum(_Sum_of_costs_Adjustado_/100),2) as costs_adj
,round(sum(_Raw_Material_USD_Adjustado_/100),2) as raw_material_adj
,round(sum(_Operating_Cost_including_Direct_Labor_USD_Adjustado_/100),2) as operating_cost_incl_dl_adj
,round(sum(_Indirect_Cost_including_OH_USD_Adjustado_/100),2) as  indirect_cost_incl_oh_adj	 
,round(sum(_DiscountRebate_Amount_/100),2) as discount_adj 	
,round(sum(Return_Tagging/100),2) as Return_Tagging
--select distinct Sum_of_Units____,kg_sales
FROM `responsive-gist-387019.tekniplex.20230519_Consolidado_Preliminar_DG` 
where entity IN('129-São Paulo, BRA','130-Winston-Salem, NC','133-Triadelphia, WV','134-Blauvelt, NY','127-Ada, MI - Proforma Fcst','114-Schaumburg, IL','115-Clinton, IL')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,final_consolidated as(
SELECT distinct safe_cast(replace(left(entity,4),"-","") as int64) as entity_code,replace(entity,',','-') as entity_name,Business_Unit,Plant_Business_line as Main_Business_Line,fy,transaction_date,CUSTOMER_ID, customer_name,customer_location
,cust_group,SKU,sku_description,product_line,product_class,product_type,primary_substrate
,end_market,Product_Class_Homologated,Product_Type_Homologated,Primary_Substrate_Homologated,End_Market_Homologated,NegocioIntercompany_Transaction_Tagging as Intercompany_Transaction_Tagging,Un_of_Measurement
,sum(quantity) as quantity,sum(Sum_of_Units) as sum_of_units,sum(Kg_sales) as sum_kg_sales 
,sum(sales) as sales, sum(costs) as costs,sum(raw_material) as raw_material,sum(operating_cost_incl_dl) as operating_cost_incl_dl, sum(indirect_cost_incl_oh) as indirect_cost_incl_oh,sum(sales_w_discount) as sales_w_discount
,sum(costs_adj) as costs_pr,sum(raw_material_adj) as raw_material_pr,sum(operating_cost_incl_dl_adj) as operating_cost_incl_dl_pr,sum(indirect_cost_incl_oh_adj) as indirect_cost_incl_oh_pr,sum(discount_adj) as discount_rebate_amount,sum(Return_Tagging) as Return_Tagging
 FROM all_fields_fix 
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,primary_substrate,17,18,19,20,21,22,23
)
###################################### IPS ########################################################
,all_fields as(
SELECT DISTINCT entity, "Integrated Performance Solutions" as Business_Unit
,case when entity in(114,115) then 'Dispensing' else 'Sealing' end as Main_Business_Line,2023 as FY
,date_add(date(concat(20,left(safe_cast(transaction_date as string),2),'-',right(safe_cast(transaction_date as string),2),'-','01')), interval -6 month) as transaction_date,CUSTOMER_ID, customer_name,Customer_GroupMaster_Account as cust_group,customer_location,SKU,Unit_of_Measurement,product__description,product__class,product_type,Primary_Substrate_,end_market,intercompany_transaction_tagging
,case when entity in(114,115) then 'Dispensing' else 'Sealing' end as product_line
,sum(Sales_Amount) as sales,sum(safe_cast(quantity as float64)) as quantity
,sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
,sum(safe_cast(DiscountRebate_Amount as float64)) as discount,sum(safe_cast(Return_Tagging as float64)) as Return_Tagging
FROM `responsive-gist-387019.tekniplex.20230613_IPS_America_v2` 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
--ORDER BY 1
)
,nulls_fixed as(
select distinct entity as entity_code
,case when entity=129 then '129-São Paulo, BRA' when entity=130 then '130-Winston-Salem, NC' when entity=133 then'133-Triadelphia, WV' when entity=134 then '134-Blauvelt, NY' when entity=127 then '127-Ada, MI - Proforma Fcst' when entity=114 then '114-Schaumburg, IL' when entity=115 then '115-Clinton, IL' else null end as entity_name,business_unit,main_business_line,fy,transaction_date
,sku,product__description as sku_description,product__class,product_type,primary_substrate_,end_market
,customer_id,cust_group,customer_name,customer_location,product_line,intercompany_transaction_tagging
,Unit_of_Measurement,sum(quantity) as quantity,sum(Sales) as sales,sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
from all_fields
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
,Unit_of_Measurement
)
,final_ips_am as(
select distinct entity_code,entity_name
,business_unit,main_business_line,fy,transaction_date
,customer_id,customer_name,customer_location,cust_group
,sku,sku_description,product_line,product__class as product_class,product_type,Primary_Substrate_ as Primary_Substrate,end_market
,'' as Product_Class_Homologated,'' as Product_Type_Homologated,'' as Primary_Substrate_Homologated,'' as End_Market_Homologated
,safe_cast(Intercompany_Transaction_Tagging as string) as Intercompany_Transaction_Tagging
,unit_of_measurement as un_of_measurement
,sum(quantity) as quantity,null as sum_of_units, null as sum_kg_sales
,sum(Sales) as sales,sum(Standard_Cost) as Costs,sum(Standard_Material) as raw_material,sum(Standard_Labor) as operating_cost_incl_dl
,sum(Standard_OH) as indirect_cost_incl_oh,sum(sales) as sales_w_discount
,null as costs_pr,null as raw_material_pr,null as operating_cost_incl_dl_pr,null as indirect_cost_incl_oh_pr,null as discount_pr
,null as Return_Tagging
from nulls_fixed
--limit 10
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
order by 1--,2
)
##############################################Union################################################
,consolidated_filtered as(
select * from final_consolidated a
where concat(a.entity_code,a.transaction_date) not in(select distinct concat(entity_code,transaction_date) from final_ips_am) 
)
,union_all as(
select * from consolidated_filtered
union all select * from final_ips_am
)
,homologation_fixes as(
select * except(primary_substrate)
,case when (Primary_Substrate is null or Primary_Substrate="#N/A") then first_value(Primary_Substrate) over(partition by sku,entity_code order by Primary_Substrate desc) 
else Primary_Substrate end as Primary_Substrate
from union_all
)
/*
select distinct
entity_code,entity_name
,fy,sku,customer_id,customer_name,Intercompany_Transaction_Tagging
,sum(sales) as sales
,sum(Discount_Rebate_Amount) as Discount_Rebate_Amount
,sum(Return_Tagging) as Discount_Rebate_Amount
,sum(costs) as total_costs
,sum(raw_material) as raw_material
,sum(operating_cost_incl_dl) as operating_cost_incl_dl
,sum(indirect_cost_incl_oh) as indirect_cost_incl_oh
from union_all
where date_trunc(transaction_date,month) between "2022-07-01" and "2023-03-01"
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7
*/
select distinct
entity_code,entity_name
--,primary_substrate
,sum(sales) as sales
,sum(Discount_Rebate_Amount) as Discount_Rebate_Amount
,sum(Return_Tagging) as Discount_Rebate_Amount
,sum(costs) as total_costs
,sum(raw_material) as raw_material
,sum(operating_cost_incl_dl) as operating_cost_incl_dl
,sum(indirect_cost_incl_oh) as indirect_cost_incl_oh
,count(distinct sku) as skus
from homologation_fixes
where date_trunc(transaction_date,month) between "2022-07-01" and "2023-03-01"
group by 1,2--,3

