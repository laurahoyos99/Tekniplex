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
select * except(primary_substrate,sku_description,product_class,product_type,end_market,cust_group,customer_name,intercompany_transaction_tagging)
,case when (Primary_Substrate is null or Primary_Substrate="#N/A") then first_value(Primary_Substrate) over(partition by sku,entity_code order by Primary_Substrate desc) else primary_substrate end as primary_substrate
,case when sku_description is null then first_value(sku_description) over(partition by sku,entity_code order by sku_description desc) else sku_description end as sku_Description
,case when product_class is null then first_value(product_class) over(partition by sku,entity_code order by product_class desc) else product_class end as product_class
,case when product_type is null then first_value(product_type) over(partition by sku,entity_code order by product_type desc) else product_type end as product_type
,case when end_market is null then first_value(end_market) over(partition by sku,entity_code order by end_market desc) else end_market end as end_market
--
,case when cust_group is null then first_value(cust_group) over(partition by customer_id,entity_code order by cust_group desc) else cust_group end as cust_group
,case when customer_name is null then first_value(customer_name) over(partition by customer_id,entity_code order by customer_name desc) else customer_name end as customer_name
,case when intercompany_transaction_tagging is null then first_value(intercompany_transaction_tagging) over(partition by customer_id,entity_code order by intercompany_transaction_tagging desc) else intercompany_transaction_tagging end as intercompany_transaction_tagging
from union_all
)
############################################### P&L ################################################
,all_fields_2 as (
  SELECT distinct entidad_codigo, Account, Tag_hierarchy_accounts as hierarchy, Variation_accounts
  , year,  mes as date_1 
  ,safe_cast(substring(cast(mes as string),9,2) as int64) AS month
  ,case when account = 'Depr_Exp - Depreciation Expense - COGS' then 'depreciation'
        when account = 'Inv_Adj - Inventory Adjustments' then 'inv_adj'
        when account = 'Freight' then 'Freight' --else variation_accounts
  end as account_fix 
  ,round(sum(usd),0) as value_usd
  FROM (select *
          , case when account='COGS_DM - Cost of Goods Sold - Direct Material' then 'Direct_Material' 
                 when account='COGS_DL - Cost of Goods Sold - Direct Labor' then 'Direct_Labor' 
                 when account= 'COGS_OH - Cost of Goods Sold - Overhead' then 'Overhead' 
                 when account='Depr_Exp - Depreciation Expense - COGS' then 'Depreciation' 
                 when account='Inv_Adj - Inventory Adjustments' then 'Inv_Adj'
            end as Variation_accounts
        from `responsive-gist-387019.tekniplex.02062023_P_L_monthly_consolidated` )
  group by 1,2,3,4,5,6,7
)
  -- (B) TABLA CON COSTOS Y VARIACIONES
, var_table as (
        select * 
        from (select entidad_codigo, variation_accounts,date_1, month, year, value_usd from all_fields_2) as selected_fields 
        pivot (sum(value_usd) for variation_accounts in ('Direct_Material','Direct_Labor','Overhead','Depreciation','Inv_Adj'))
)
############################################# Pro-Rata ###############################################
,template_per_month as(
select distinct extract(year from transaction_date) as year,extract(month from transaction_date) as month,entity_code
--Ventas ya cuadran
  ,ROUND(sum(raw_material),1) as rm_total_month
  ,ROUND(sum(operating_cost_incl_dl),1) as dl_total_month
  ,ROUND(sum(indirect_cost_incl_oh),1) as oh_total_month
  ,Round(sum(costs),1) as total_cost_month
from homologation_fixes
group by 1,2,3
)
,table_porc as(
  Select a.*
   ,round(((a.raw_material/b.rm_total_month)),5) as rm_porc
   ,round(((a.operating_cost_incl_dl/b.dl_total_month)),5) as dl_porc
   ,round(((a.indirect_cost_incl_oh/b.oh_total_month)),5) as oh_porc
 from homologation_fixes a left join template_per_month b on extract(month from a.transaction_date)=b.month and extract(year from a.transaction_date)=b.year and a.entity_code=b.entity_code
)
,definitive_IPS_PR as(
  SELECT a.*
      ,case when (entity_code=114 and left(sku_description,3) IN('F21','TB8')) then 'PE' 
          when (entity_code=114 and left(sku_description,2)='S8') then 'PE' 
          when primary_substrate='OTHER' then 'Other' else primary_substrate end as primary_substrate_adj
  ,round(coalesce((b.Direct_Material * a.rm_porc),0),1) as RM_Adj
  ,round(coalesce((b.Direct_Labor * a.dl_porc),0),1) as dl_Adj
  ,round(coalesce((b.Overhead * a.oh_porc),0),1) as oh_Adj
  ,round(coalesce((b.Inv_Adj * a.oh_porc),0),1) as Inv_a_Adj
  from table_porc a left join var_table b on a.entity_code=b.entidad_codigo and extract(month from a.transaction_date)=b.month 
  and extract(year from a.transaction_date)=b.year
)
select distinct entity_code,entity_name
,date_trunc(transaction_date,month) as month
--,product_type,primary_substrate,primary_substrate_adj
--,product_class,product_type,SKU,sku_description
,intercompany_transaction_tagging,customer_id,customer_name
--,round(sum(Sales),2) as sales,round(sum(sales),2) as sales_w_discount,round(sum(raw_Material),2) as raw_material
,sum(sales) as sales,sum(rm_adj) as rm_adj,sum(dl_adj) as dl_adj, sum(oh_adj) as oh_adj,sum(inv_a_adj) as inv_adj
from definitive_IPS_PR
where date_trunc(transaction_date,month) between date('2022-10-01') and date('2023-03-01')
group by 1,2,3,4,5,6--,7,8,9,10,11
order by 1,2

