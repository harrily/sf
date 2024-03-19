--航空拖寄物

1.运单宽表
dwd.dwd_waybill_info_dtl_di
--取出所有航空件的运单号
waybill_no     --


case when t1.product_code = 'SE0005' and (nvl(t1.route_code,'') not in ('T6','ZT6') or t1.route_code is null) then '1'
。。。
where inc_day -- between '20230101' and '20230906'
	between '$[time(yyyyMMdd,-7d)]' and '$[time(yyyyMMdd,-1d)]' ---between '20220901' and '20220922'
	and product_code in ('SE000201','SE0001','SE0107','SE0103','SE0051','SE0089','SE0109','SE0008','SE0137','SE0121','SE0004','SE0146','SE0152','SE000206','SE0153','SE0005')
	and concat(nvl(src_dist_code,'#'),'-',nvl(dest_dist_code,'#')) in (select cityflow from dm_ordi_predict.cf_air_list)
	
	
2.dwd.dwd_waybill_consign_category_info_di
--拖寄物明细表

inc_day
waybill_no
first_classify   21    一个运单两个分类
second_        
third_


匹配拖寄物类型


3.匹配中文名称，并汇总到省维度
dm_ordi_predict.dim_city_level_mapping_df

city_code
city_name
province
area_name

