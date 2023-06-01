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
where entity IN('101-Decatur, IN','102-Dallas, TX','103-Lawrenceville, GA','105-Wenatchee, WA','106-Bloomfield, NY')
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,final_consolidated as(
SELECT distinct safe_cast(replace(left(entity,4),"-","") as int64) as entity_code,replace(entity,',','-') as entity_name,Business_Unit,Plant_Business_line as Main_Business_Line,fy,transaction_date,CUSTOMER_ID, customer_name,customer_location
,cust_group,SKU,sku_description,product_line,product_class,product_type,Primary_Substrate,end_market,Product_Class_Homologated,Product_Type_Homologated,Primary_Substrate_Homologated,End_Market_Homologated,NegocioIntercompany_Transaction_Tagging as Intercompany_Transaction_Tagging,Un_of_Measurement
,sum(quantity) as quantity,sum(Sum_of_Units) as sum_of_units,sum(Kg_sales) as sum_kg_sales 
,sum(sales) as sales, sum(costs) as costs,sum(raw_material) as raw_material,sum(operating_cost_incl_dl) as operating_cost_incl_dl, sum(indirect_cost_incl_oh) as indirect_cost_incl_oh,sum(sales_w_discount) as sales_w_discount
,sum(costs_adj) as costs_pr,sum(raw_material_adj) as raw_material_pr,sum(operating_cost_incl_dl_adj) as operating_cost_incl_dl_pr,sum(indirect_cost_incl_oh_adj) as indirect_cost_incl_oh_pr,sum(discount_adj) as discount_rebate_amount,sum(Return_Tagging) as Return_Tagging
 FROM all_fields_fix 
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
######################################## FY 2023 ##############################################
,all_fields_FY23 as(
  select distinct entity as entity_code, Business_Unit as business_unit
  ,case when entity = 101 THEN '101-Decatur- IN'
        when entity = 102 THEN '102-Dallas- TX'
        when entity = 103 THEN '103-Lawrenceville- GA'
        when entity = 105 THEN '105-Wenatchee- WA'
        when entity = 106 THEN '106-Bloomfield- NY'
  end as entity_name
  ,date(transaction_date) as transaction_date,2023 as FY
  ,Customer_ID,Customer_Name,Customer_GroupMaster_Account AS cust_group,Customer_Location
  ,_Intercompany_Transaction_Tagging_ as intercompany_transaction_tagging
  ,End_Market,SKU,Trim(upper(Unit_of_Measurement)) as unit_of_measurement,Product__Description as sku_description
  ,Product__Class as product_class,Product_Type,Primary_Substrate_ as primary_substrate
  ,SUM(quantity) AS quantity,SUM(Sales_Amount) AS sales,Sum(Standard_Cost) AS Standard_Cost
  ,SUM(Standard_Material) AS Standard_Material,SUM(Standard_Labor) AS Standard_Labor,SUM(Standard_OH) AS Standard_OH
  ,SUM(DiscountRebate_Amount) as Discount_Rebate_Amount,SUM(Return_Tagging) as Return_Tagging
FROM `responsive-gist-387019.tekniplex.20230531_FY_2023_All_101-106` 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
-- Final table:
,final_101_to_106 as(
  Select distinct entity_code,entity_name,'Fresh Foods Solutions' as Business_Unit
  ,'Foam' as Main_Business_Line,FY,transaction_date
  ,Customer_ID,Customer_Name,Customer_Location,cust_group,SKU,sku_description,'' as Product_line,product_class
  ,Product_Type,primary_substrate,End_Market,'' as Product_Class_Homologated,'N/A' as Product_Type_Homologated
  ,'' as Primary_Substrate_Homologated,'' as End_Market_Homologated,Intercompany_Transaction_Tagging
  ,Unit_of_Measurement as UN_of_measurement,round(Quantity,2) quantity,null as Sum_of_units, null as Kg_sales
  ,round((sales),2) as Sales_USD
  ,Round((COALESCE(Standard_material, 0) + COALESCE(standard_labor, 0) + COALESCE(standard_OH,  
   0) ),2) as sum_of_costs,Round((standard_material),2) as Raw_Material_USD
  ,Round((standard_labor),2) as Operating_Cost_including_dl_USD
  ,Round((standard_OH),2) as Indirect_Cost_including_OH__USD
  ,Round((sales - Discount_Rebate_Amount ),2) as Sales_USD_w_discount
  ,null as Sum_of_costs_adj,null as Raw_Material_USD_adj,null as Operating_Cost_including_Direct_Labor_USD_adj
  ,null as Indirect_Cost_including_OH_USD_adj,Discount_Rebate_Amount,Return_Tagging
  from all_fields_FY23
  order by 1
)
##############################################Union################################################
,consolidated_filtered as(
select * from final_consolidated a
where concat(a.entity_code,a.transaction_date) not in(select distinct concat(entity_code,transaction_date) from final_101_to_106) --172 y 174 es solo 2023
)
,union_all as(
select * from consolidated_filtered
union all select * from final_101_to_106
)


select distinct primary_substrate
/*entity_code,entity_name
,fy,sku,customer_id,customer_name,Intercompany_Transaction_Tagging
,sum(sales) as sales
,sum(Discount_Rebate_Amount) as Discount_Rebate_Amount
,sum(Return_Tagging) as Discount_Rebate_Amount
,sum(costs) as total_costs
,sum(raw_material) as raw_material
,sum(operating_cost_incl_dl) as operating_cost_incl_dl
,sum(indirect_cost_incl_oh) as indirect_cost_incl_oh*/
from union_all
--final_consolidated
--group by 1,2,3 
--order by 1,2,3
--where fy=2023
--where date_trunc(transaction_date,month) between "2022-07-01" and "2023-03-01"
--group by 1,2,3,4,5,6,7
--order by 1,2,3,4,5,6,7

