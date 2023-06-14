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
,cust_group,SKU,sku_description,product_line,product_class,product_type,primary_substrate
,end_market,Product_Class_Homologated,Product_Type_Homologated,Primary_Substrate_Homologated,End_Market_Homologated,NegocioIntercompany_Transaction_Tagging as Intercompany_Transaction_Tagging,Un_of_Measurement
,sum(quantity) as quantity,sum(Sum_of_Units) as sum_of_units,sum(Kg_sales) as sum_kg_sales 
,sum(sales) as sales, sum(costs) as costs,sum(raw_material) as raw_material,sum(operating_cost_incl_dl) as operating_cost_incl_dl, sum(indirect_cost_incl_oh) as indirect_cost_incl_oh,sum(sales_w_discount) as sales_w_discount
,sum(costs_adj) as costs_pr,sum(raw_material_adj) as raw_material_pr,sum(operating_cost_incl_dl_adj) as operating_cost_incl_dl_pr,sum(indirect_cost_incl_oh_adj) as indirect_cost_incl_oh_pr,sum(discount_adj) as discount_rebate_amount,sum(Return_Tagging) as Return_Tagging
 FROM all_fields_fix 
 group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,primary_substrate,17,18,19,20,21,22,23
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
  ,Product__Class as product_class,Product_Type,primary_substrate_ as primary_substrate
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
,return_tagging+discount_rebate_amount as discounts
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
                 when account='Sales_Disc - Discounts & Allowances' then 'Discounts'
                 when account in('Sales_IC - Sales - Intercompany','Sales_Ext - Sales - External') then 'Sales_WO_Discounts'
            end as Variation_accounts
        from `responsive-gist-387019.tekniplex.02062023_P_L_monthly_consolidated` )
  group by 1,2,3,4,5,6,7
)
  -- (B) TABLA CON COSTOS Y VARIACIONES
, var_table as (
        select * 
        from (select entidad_codigo, variation_accounts,date_1, month, year, value_usd from all_fields_2) as selected_fields 
        pivot (sum(value_usd) for variation_accounts in ('Direct_Material','Direct_Labor','Overhead','Depreciation','Inv_Adj','Sales_WO_Discounts','Discounts'))
)
############################################# Pro-Rata ###############################################
,template_per_month as(
select distinct extract(year from transaction_date) as year,extract(month from transaction_date) as month,entity_code
--Ventas ya cuadran
  ,ROUND(sum(raw_material),1) as rm_total_month
  ,ROUND(sum(operating_cost_incl_dl),1) as dl_total_month
  ,ROUND(sum(indirect_cost_incl_oh),1) as oh_total_month
  ,Round(sum(costs),1) as total_cost_month
  ,Round(sum(sales),1) as total_sales_month
  ,Round(sum(Discount_Rebate_Amount)+sum(Return_Tagging),1) as total_discount_month
from homologation_fixes
group by 1,2,3
)
,table_porc as(
  Select a.*
   ,round(((a.raw_material/b.rm_total_month)),5) as rm_porc
   ,round(((a.operating_cost_incl_dl/b.dl_total_month)),5) as dl_porc
   ,round(((a.indirect_cost_incl_oh/b.oh_total_month)),5) as oh_porc
   ,round(((a.sales/b.total_sales_month)),5) as sales_porc
   ,round(((a.discounts/b.total_discount_month)),5) as disc_porc
 from homologation_fixes a left join template_per_month b on extract(month from a.transaction_date)=b.month and extract(year from a.transaction_date)=b.year and a.entity_code=b.entity_code
)
,definitive_FFNA_PR as(
  SELECT a.*
  ,round(coalesce((b.Direct_Material * a.rm_porc),0),1) as RM_Adj
  ,round(coalesce((b.Direct_Labor * a.dl_porc),0),1) as dl_Adj
  ,round(coalesce((b.Overhead * a.oh_porc),0),1) as oh_Adj
  ,round(coalesce((b.Inv_Adj * a.oh_porc),0),1) as Inv_a_Adj
  ,round(coalesce((b.Discounts * a.disc_porc),0),1) as Disc_Adj
  ,round(coalesce((b.Sales_WO_Discounts * a.sales_porc),0),1) as Sales_WO_Dis_Adj
  from table_porc a left join var_table b on a.entity_code=b.entidad_codigo and extract(month from a.transaction_date)=b.month 
  and extract(year from a.transaction_date)=b.year
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
entity_code,entity_name,SKU,sku_description,intercompany_transaction_tagging,customer_id,customer_name
,sum(sales) as sales,sum(rm_adj) as rm_adj,sum(dl_adj) as dl_adj, sum(oh_adj) as oh_adj,sum(inv_a_adj) as inv_adj
from definitive_FFNA_PR
where transaction_date between date('2022-07-01') and date('2023-03-01')
group by 1,2,3,4,5,6,7
order by 1,2

