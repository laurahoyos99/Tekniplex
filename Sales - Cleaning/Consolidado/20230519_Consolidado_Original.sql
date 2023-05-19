with all_fields_fix as(
select distinct entity
,date(concat(year,'-',lpad(safe_cast(month as string),2,'0'),'-','01')) as transaction_date, fy
,CUSTOMER_ID, customer_name,Customer_GroupMaster_Account as cust_group,SKU,Un_of_Measurement,description,_product_line_,product_class,product_type,Primary_Substrate_,end_market
,sum(quantity) as quantity
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
,round(sum(Return_Tagging/100),2)
FROM `responsive-gist-387019.tekniplex.20230519_Consolidado_Preliminar_DG` 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
SELECT distinct replace(entity,',','-') as entity,fy,sum(sales) as sales, sum(costs) as costs,sum(raw_material) as raw_material,sum(operating_cost_incl_dl) as operating_cost_incl_dl, sum(indirect_cost_incl_oh) as indirect_cost_incl_oh,sum(sales_w_discount) as sales_w_discount
--entity, sum(sales_fix),sum(costs_fix)
 FROM all_fields_fix 
 group by 1,2
 order by 1,2
