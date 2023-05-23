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
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22
)
,final_consolidated as(
SELECT distinct safe_cast(replace(left(entity,4),"-","") as int64) as entity_code,replace(entity,',','-') as entity_name,Business_Unit,Plant_Business_line as Main_Business_Line,fy,transaction_date,CUSTOMER_ID, customer_name,customer_location
,cust_group,SKU,sku_description,product_line,product_class,product_type,Primary_Substrate,end_market,Product_Class_Homologated,Product_Type_Homologated,Primary_Substrate_Homologated,End_Market_Homologated,NegocioIntercompany_Transaction_Tagging as Intercompany_Transaction_Tagging,Un_of_Measurement
,sum(quantity) as quantity,sum(Sum_of_Units) as sum_of_units,sum(Kg_sales) as sum_kg_sales 
,sum(sales) as sales, sum(costs) as costs,sum(raw_material) as raw_material,sum(operating_cost_incl_dl) as operating_cost_incl_dl, sum(indirect_cost_incl_oh) as indirect_cost_incl_oh,sum(sales_w_discount) as sales_w_discount
,sum(costs_adj) as costs_pr,sum(raw_material_adj) as raw_material_pr,sum(operating_cost_incl_dl_adj) as operating_cost_incl_dl_pr,sum(indirect_cost_incl_oh_adj) as indirect_cost_incl_oh_pr,sum(discount_adj) as discount_pr,sum(Return_Tagging) as Return_Tagging
 FROM all_fields_fix 
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
###########################################IPS North America#################################################
,ips_na_all_fields as(
SELECT DISTINCT entity as entity_code
,case when entity=114 then '114-Schaumburg- IL' when entity=115 then '115-Clinton- IL' when entity=127 then '127-Ada- MI - Proforma Fcst' when entity=129 then '129-São Paulo- BRA'when entity=130 then '130-Winston-Salem- NC' when entity=133 then '133-Triadelphia- WV' when entity=134 then '134-Blauvelt- NY' else null end as entity_name
, "Integrated Performance Solutions" as Business_Unit
,case when entity in(114,115) then 'Dispensing' else 'Sealing' end as Main_Business_Line,2023 as FY
,date_add(date(concat(20,left(safe_cast(transaction_date as string),2),'-',right(safe_cast(transaction_date as string),2),'-','01')), interval -6 month) as transaction_date,CUSTOMER_ID, customer_name,Customer_Group_Master_Account as cust_group,customer_location,SKU,Unit_of_Measurement,product__description,product__class,product_type,Primary_Substrate_,end_market
,case when entity in(114,115) then 'Dispensing' else 'Sealing' end as product_line
,sum(Sales_Amount) as sales
,sum(safe_cast(quantity_f as float64)) as quantity_f
,sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
,sum(safe_cast(Discount_Rebate_Amount as float64)) as discount,sum(safe_cast(Return_Tagging as float64)) as Return_Tagging
from(select *
,case when quantity like '%(%' then concat('-',right(left(quantity,length(quantity)-1),length(quantity)-3))
when quantity='-' then '0' when quantity like '%.%' then replace(quantity,'.','') else quantity end as quantity_f
FROM `responsive-gist-387019.tekniplex.20230517_FY2023_114_a_134` )
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
,ips_na_nulls_fixed as(
select distinct entity_code,entity_name,business_unit,main_business_line,fy,transaction_date
,sku,product__description as sku_description,product__class,product_type,primary_substrate_,end_market
,customer_id,cust_group,customer_name,customer_location,product_line
,case when cust_group is null then first_value(cust_group) over(partition by customer_id order by cust_group desc) else cust_group end as fix_cust_group
,case when customer_name is null then first_value(customer_name) over(partition by customer_id order by customer_name desc) else customer_name end as fix_customer_name
,case when customer_location is null then first_value(customer_location) over(partition by customer_id order by customer_location desc) else customer_location end as fix_customer_location
,Unit_of_Measurement
,sum(quantity_f) as quantity,sum(Sales) as sales,sum(Standard_Cost) as Standard_Cost, sum(Standard_Labor) as Standard_Labor,sum(Standard_Material) as Standard_Material,sum(Standard_OH) as Standard_OH
from ips_na_all_fields
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,Unit_of_Measurement
)
,final_ips_na as(
select distinct entity_code,entity_name,business_unit,main_business_line,fy,transaction_date
,customer_id,fix_customer_name as customer_name,fix_customer_location as customer_location,fix_cust_group as cust_group,sku,sku_description,product_line,product__class as product_class,product_type,Primary_Substrate_ as Primary_Substrate,end_market
,"" as Product_Class_Homologated,"" as Product_Type_Homologated,"" as Primary_Substrate_Homologated,"" as End_Market_Homologated,"" as Intercompany_Transaction_Tagging,unit_of_measurement as un_of_measurement
,sum(quantity) as quantity,null as sum_of_units, null as kg_sales,sum(Sales) as sales,sum(Standard_Cost) as Costs,sum(Standard_Material) as raw_material,sum(Standard_Labor) as operating_cost_incl_dl,sum(Standard_OH) as indirect_cost_incl_oh
,sum(sales) as sales_w_discount
,null as costs_pr,null as raw_material_pr,null as operating_cost_incl_dl_pr,null as indirect_cost_incl_oh_pr,null as discount_pr,null as Return_Tagging
from ips_na_nulls_fixed
--limit 10
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
order by 1--,2
)
##########################################172-Begium###################################################
,all_fields_172 as(
SELECT DISTINCT ENTITY,EXTRACT(year FROM date(Transaction_Date) ) AS Year,EXTRACT(MONTH FROM date(Transaction_Date) ) AS month,Customer_ID,Customer_Name, Customer_Group_Master_Account as cust_group,  End_Market,SKU,Unit_of_Measurement, Product__Description, Product__Class, Product_Type, Primary_Substrate_fix,currency,sum(Quantity) as quantity,sum(Sales_Amount) as sales,sum(Standard_Material) as Standard_Material,sum(Standard_Labor)as Standard_Labor,sum(Standard_Overhead)as Standard_OH
FROM(select *,first_value(primary_substrate_) over(partition by SKU order by EXTRACT(MONTH FROM date(Transaction_Date) ) desc) as Primary_Substrate_fix
from`responsive-gist-387019.tekniplex.172_FY2023`) group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
,discount_fields_172 as (  
SELECT distinct ENTITY,FY as Year,safe_cast(LEFT(Transaction_Date, 2) as float64) as month,Customer_Name, SKU,currency,sum(Sales_Amount) as Sales_2,sum(Return) as return,sum(Cash_Discount) as cash_discount,sum(Rebate) as rebate,sum(Net_Sales) as net_sales
FROM `responsive-gist-387019.tekniplex.172_discounts` 
group by 1,2,3,4,5,6
)
,join_disc_172 as(
select *,(sales/TC) as Sales_,((Standard_material + standard_labor + standard_OH )/TC) as costs,(standard_material/TC) as Raw_Material,(standard_labor/TC) as operating_cost_incl_dl,(standard_OH/ TC) as indirect_cost_incl_oh,(net_sales/TC) as sales_w_discount,(cash_discount+rebate) as Discount_pr from(
select a.*,b.return,b.cash_discount,b.rebate,b.net_sales
,case when safe_cast(a.month as string)= '1' then 1.0222718
  when safe_cast(a.month as string) = '2' then 1.028073
  when safe_cast(a.month as string) = '3' then 1.0325446
  end as TC
from all_fields_172 a left join discount_fields_172 b on a.entity=b.entity  and a.year=b.year  and safe_cast(a.month as string) = safe_cast(b.month as string) and a.sku= b.sku and a.customer_name= b.customer_name)
)
,final_172_FY23 as(
select distinct 172 as entity_code,concat(entity, '-Belgium Dispensing') as Entity_name,'Integrated Performance Solutions' as Business_Unit,'Dispensing' as Main_Business_line
,2023 as FY,date(concat(year,'-',lpad(safe_cast(month as string),2,'0'),'-','01')) as transaction_date
,safe_cast(CUSTOMER_ID as string) as customer_id, customer_name,'' as customer_location,cust_group,SKU,product__description as sku_description,'' as product_line,Product__Class as product_class,product_type,Primary_Substrate_fix as Primary_Substrate,end_market
,"" as Product_Class_Homologated,"" as Product_Type_Homologated,"" as Primary_Substrate_Homologated,"" as End_Market_Homologated,"" as Intercompany_Transaction_Tagging,unit_of_measurement as un_of_measurement
,sum(quantity) as quantity,null as Sum_of_Units,null as Kg_sales,sum(sales_) as Sales,sum(costs) as costs,sum(raw_material) as Raw_Material,sum(operating_cost_incl_dl) as operating_cost_incl_dl,sum(indirect_cost_incl_oh) as indirect_cost_incl_oh,sum(sales_w_discount) as sales_w_discount,null as costs_pr,null as raw_material_pr,null as operating_cost_incl_dl_pr,null as indirect_cost_incl_oh_pr
--en consolidado esta como ajustado entonces no se
,sum(discount_pr) as Discount_pr,sum(return) as Return_Tagging
from join_disc_172 where entity is not null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
###########################################174-Italy################################################
,all_fields_174 as (
SELECT  ENTITY,Business_Unit,fy,FY AS Year,Month,Customer_ID,Customer_Name, Customer_Group_Master_Account, 
  Customer_Location,Intercompany_Transaction_Tagging,End_Market,SKU, Unit_of_Measurement, Product__Description, Product__Class, Product_Type, Primary_Substrate_,currency
  ,case when month = 1 then 1.0222718 when month = 2 then 1.028073 when month = 3 then 1.0325446 end as TC
  ,sum(Quantity) as quantity,sum(Sales_Amount) as sales,sum(Standard_Material) as Standard_Material,sum(Standard_Labor)as Standard_Labor,sum(Standard_Overhead)as Standard_OH,sum(Discount_Rebate_Amount) as Discount_Rebate_Amount,sum( cast(Return_Tagging as float64)) as Return_Tagging
FROM (select *,case when LEFT(Transaction_Date, 3)='ene' then 1 when LEFT(Transaction_Date, 3)='feb' then 2 when LEFT(Transaction_Date, 3)='mar' then 3 end as month
from `responsive-gist-387019.tekniplex.05192023_FY2023Q3_174` )
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
,final_174 as(
select distinct entity as entity_code,concat( entity, '-Gaggiano- ITA') as Entity_name,Business_Unit,'Dispensing' as Main_Business_line,FY,date(concat(year,'-',lpad(safe_cast(month as string),2,'0'),'-','01')) as transaction_date,safe_cast(Customer_ID as string) as customer_id,Customer_name,Customer_Location,Customer_Group_Master_Account as cust_group,SKU as SKU,product__description as sku_Description, '' as Product_line,Product__Class as Product_Class,Product_Type,Primary_Substrate_ as primary_substrate,End_Market
 ,'' as Product_Class_Homologated,'' as Product_Type_Homologated,'' as Primary_Substrate_Homologated, '' as End_Market_Homologated,Intercompany_Transaction_Tagging,Unit_of_Measurement as UN_of_Measurement
,sum(Quantity) as quantity,null as Sum_of_Units,null as sum_Kg_sales,sum(Sales_USD) as sales,sum(Sum_of_costs) as costs,sum(Raw_Material_USD) as raw_material,sum(st_labor_tc) as operating_cost_incl_dl,sum(ind_costs) as indirect_cost_incl_oh,sum(Sales_USD_w_discount) as sales_w_discount,null as costs_pr,null as Raw_Material_pr,null as operating_cost_incl_dl_pr,null as indirect_cost_incl_oh_pr,sum(Discount_Rebate_Amount) as discount_pr,sum(Return_Tagging) as return_tagging
from(select *,(sales/TC) as Sales_USD,((Standard_material + standard_labor + standard_OH )/TC) as Sum_of_costs,(standard_material/TC) as Raw_Material_USD,(standard_labor/TC) as st_labor_tc,(standard_OH/ TC) as ind_costs,((sales-discount_rebate_amount)/TC) as Sales_USD_w_discount
from all_fields_174)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
)
##############################################Union################################################
,consolidated_filtered as(
select * from final_consolidated a
where concat(a.entity_code,a.transaction_date) not in(select distinct concat(entity_code,transaction_date) from final_ips_na) --172 y 174 es solo 2023
)
,union_all as(
select * from consolidated_filtered
union all select * from final_ips_na
union all select * from final_172_FY23
union all select * from final_174
)

select distinct --* -- 
--entity_code,entity_name,fy,transaction_date
entity_code,	fy
,sum(sales) as sales,sum(sales_w_discount) as sales_w_discount
--business_unit,entity_code,entity_name
from union_all
--where fy=2023
where entity_code=174
group by 1,2
order by 1,2
