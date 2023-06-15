with
all_fields as(
SELECT DISTINCT ENTITY,Year,right(FY, 4) as FY, _Month as month, Business_Unit,Customer_ID,Customer_Name, Customer_location,Customer_Group_Master_Account as cust_group, End_Market
,Intercompany_Transaction_Tagging,SKU, Unit_of_Measurement, Product__Description, Product__Class
,case when product__class = 'Cups' or product__class = 'cups' then 'Cups' 
      when product__class = 'CPG Containers' or product__class = '`' then 'CPG Containers'
      when product__class = 'Other CP' then 'Other CP'
      when product__class = 'Other HC' then 'Other HC'
      when product__class = 'N/A' then 'N/A'
      when product__class = 'Lidding' then 'Lidding'
      when product__class = 'Roll Stock' then 'Roll Stock'
      when product__class = 'Cuttlery' then 'Cuttlery'
      when product__class = 'Clamshells & Trays' then 'Clamshells & Trays'
end as product_class_H
,Product_Type, Primary_Substrate_fix,currency
,sum(Quantity) as quantity,sum(Sales_Amount) as sales,sum(Raw_Material_Cost) as Standard_Material,sum(Direct_Labor)as Standard_Labor,sum(Overhead)as Standard_OH
,sum(Discount_Rebate_Amount) as Discount_Rebate_Amount,sum(Return_Tagging) as _Return_Tagging_,sum(WIP_MAT) as Total_variation
 FROM(select *,first_value(primary_substrate_) over(partition by SKU order by EXTRACT(MONTH FROM date(Transaction_Date) ) desc) as Primary_Substrate_fix,
  from `responsive-gist-387019.tekniplex.20230523_PPO`)
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)
,final_PPO as(
 select *,case when entity_split='Comercializados_Otros' then 'Virginia' else entity_split end as ppo_split 
 from( select concat( entity, '-PPO') as entity_name
 ,case when lower(left(sku,1)) = 'a' then 'Arizona'
       when lower(left(sku,1)) = 'o' then 'Virginia'
       else 'Comercializados_Otros' 
  end as entity_split
 ,Business_Unit,'Plastic' as Plant_Business_line, date(concat(year,'-',month,'-','01')) as transaction_date,safe_cast(FY as int64) as fy,SKU as SKU,product__description as sku_description
 ,Customer_ID,cust_group,Customer_name, Customer_Location, Product__Class as Product_Class, Product_Type,Primary_Substrate_fix as Primary_Substrate, End_Market
 ,case when product_class_H = 'Other CP' or product_class_H = 'Other HC' then 'Other'
       when primary_substrate_fix = 'Aluminum (Foil)' then 'Aluminio'
       when primary_substrate_fix = 'Paper' then 'Paper'
       when primary_substrate_fix = 'PS' or primary_substrate_fix = 'PET' or primary_substrate_fix = 'PP' or primary_substrate_fix = 'PE' THEN 'Plastic'
       when primary_substrate_fix = 'Other' then 'Other'
  end as Product_line
  ,Product_Class_H,'N/A' as Product_Type_H, primary_substrate_fix as Primary_Substrate_H,end_market as End_Market_H,Intercompany_Transaction_Tagging as Negocio_Intercompany_Transaction_Tagging
  ,Quantity,Unit_of_Measurement as UN_of_Measurement
  ,null as Sum_of_Units____,null as Kg_sales,(sales) as Sales
  ,((COALESCE(Standard_material, 0) + COALESCE(standard_labor, 0) + COALESCE(standard_OH, 0) )) as Sum_of_costs
  ,(standard_material) as Raw_Material,(standard_labor) as operating_cost_incl_dl
  ,(standard_OH) as indirect_cost_incl_oh,((coalesce(sales,0) + COALESCE(Discount_Rebate_Amount, 0) + COALESCE(_Return_Tagging_, 0))) as Sales_w_discount
  ,null as Sum_of_costs__Adjustado_,null as Raw_Material_USD__Adjustado_, null as Operating_Cost__including_Direct_Labor__USD__Adjustado_,null as Indirect_Cost__including_OH__USD__Adjustado_
  ,Discount_Rebate_Amount,_Return_Tagging_,null as Variacion_Raw_Material, null as Variation_X_Operation,Total_variation
from all_fields)
)
,table_porc_var as(
  Select a.*
   ,round((safe_divide(a.raw_material,sum_of_costs)),5) as rm_porc_var
   ,round((safe_divide(a.operating_cost_incl_dl,sum_of_costs)),5) as dl_porc_var
   ,round((safe_divide(a.indirect_cost_incl_oh,sum_of_costs)),5) as oh_porc_var
 from final_ppo a 
)
,final_variations as(
  SELECT a.*
  ,round(coalesce((a.total_variation * a.rm_porc_var),0),1)+round(coalesce((a.raw_material),0),1) as RM_VAR
  ,round(coalesce((a.total_variation * a.dl_porc_var),0),1)+round(coalesce((a.operating_cost_incl_dl),0),1) as dl_VAR
  ,round(coalesce((a.total_variation * a.oh_porc_var),0),1)+round(coalesce((a.indirect_cost_incl_oh),0),1) as oh_VAR
  from table_porc_var a 
)

############################################### P&L ################################################
,all_fields_2 as (
  SELECT distinct Split as ppo_split, Account, Variation_accounts
  , left(safe_cast(month as string),4) as year,  month as date_1 
  , extract(month from month) AS month
  ,round(sum(value),0) as value_usd
  FROM (select *
          , case when account='COGS_DM - Cost of Goods Sold - Direct Material' then 'Direct_Material' 
                 when account='COGS_DL - Cost of Goods Sold - Direct Labor' then 'Direct_Labor' 
                 when account= 'COGS_OH - Cost of Goods Sold - Overhead' then 'Overhead' 
                 when account='Depr_Exp - Depreciation Expense - COGS' then 'Depreciation' 
                 when account='Inv_Adj - Inventory Adjustments' then 'Inv_Adj'
                 when account='Sales_Net - Net Sales' then 'Net_Sales'
            end as Variation_accounts
        from  `responsive-gist-387019.tekniplex.20230614_PPO_Split_PL`)
  group by 1,2,3,4,5,6
)
  -- (B) TABLA CON COSTOS Y VARIACIONES
, var_table as (
        select * 
        from (select ppo_split, variation_accounts,date_1, month, year, value_usd from all_fields_2) as selected_fields 
        pivot (sum(value_usd) for variation_accounts in ('Direct_Material','Direct_Labor','Overhead','Depreciation','Inv_Adj','Net_Sales'))
)
############################################# Pro-Rata ###############################################
,template_per_month as(
select distinct extract(year from transaction_date) as year,extract(month from transaction_date) as month,ppo_split
--Ventas ya cuadran
  ,ROUND(sum(rm_var),1) as rm_total_month
  ,ROUND(sum(dl_var),1) as dl_total_month
  ,ROUND(sum(oh_var),1) as oh_total_month
  ,Round(sum(sum_of_costs),1) as total_cost_month
  ,Round(sum(Sales_w_discount),1) as total_sales_w_disc
from final_variations
group by 1,2,3
)
,table_porc as(
  Select a.*
   ,round(((a.rm_var/b.rm_total_month)),5) as rm_porc
   ,round(((a.dl_var/b.dl_total_month)),5) as dl_porc
   ,round(((a.oh_var/b.oh_total_month)),5) as oh_porc
   ,round(((a.sales_w_discount/b.total_sales_w_disc)),5) as sales_porc
 from final_variations a left join template_per_month b on extract(month from a.transaction_date)=b.month and extract(year from a.transaction_date)=b.year and a.ppo_split=b.ppo_split
)
,definitive_PPO_PR as(
  SELECT a.*
  ,round(coalesce((b.Direct_Material * a.rm_porc),0),1) as RM_Adj
  ,round(coalesce((b.Direct_Labor * a.dl_porc),0),1) as dl_Adj
  ,round(coalesce((b.Overhead * a.oh_porc),0),1) as oh_Adj
  ,round(coalesce((b.Inv_Adj * a.oh_porc),0),1) as Inv_a_Adj
  ,round(coalesce((b.Net_Sales * a.sales_porc),0),1) as Sales_adj
  from table_porc a left join var_table b on a.ppo_split=b.ppo_split and extract(month from a.transaction_date)=b.month 
  and extract(year from a.transaction_date)=safe_cast(b.year as int64)
)

--/*
select distinct entity_name,ppo_split--,transaction_date
--,sku,sku_description
--,customer_id,customer_name,negocio_Intercompany_Transaction_Tagging
--,primary_substrate
,sum(sales_adj) as sales,sum(rm_adj) as rm_adj,sum(dl_adj) as dl_adj, sum(oh_adj) as oh_adj,
from definitive_ppo_pr
where date_trunc(transaction_date,month) between date('2022-10-01') and date('2023-03-01')
group by 1,2--,3--,4,5,6,7
order by 1,2--,3
