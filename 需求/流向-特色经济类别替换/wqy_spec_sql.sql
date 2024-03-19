drop table if exists tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp1;
        create table tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp1  stored as parquet as
select   waybill_no, -- 运单号
         cons_name,     -- 托寄物名
         pro_name,     -- 特色经济项目名称
         first_classify, --  一级分类
         second_classify, --  二级分类
         third_classify,     --  三级分类
   t.INC_DAY,
   row_number() over( partition by t.waybill_no order by t.INC_DAY desc) as rn
from    dm_bdp_datamining.spe_waybill_category  t
where   t.INC_DAY>='$[time(yyyyMMdd,-10d)]' 
  and   t.INC_DAY< '$[time(yyyyMMdd)]'
;  
   

drop table if exists tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp2;
        create table tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp2  stored as parquet as
select   t.waybill_no,                                        --运单号    
         t.product_code,
         t.source_zone_code,                                  --原寄件网点
      t.dest_zone_code,
         t.consigned_tm,                                      --寄件时间
         t.meterage_weight_qty,                                  --计费总重量
         t.real_weight_qty,                                      --实际重量
      t.consignor_addr_native,                                --寄方客户编码
      t.quantity,                                             --包裹总件数
      t.consignee_emp_code,                                   --收件员
      t.deliver_emp_code,                                     --派件员
      t.freight_monthly_acct_code,                            --运费月结账号
         t.inputer_emp_code,
         t.signin_tm,
      t.limit_type_code,
   t.inc_day,
   w.cons_name,     -- 托寄物名
         w.pro_name,     -- 特色经济项目名称
         w.first_classify, --  一级分类
         w.second_classify, --  二级分类
         w.third_classify     --  三级分类
 from   dwd.dwd_waybill_info_dtl_di  t
 inner join tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp1 w on t.waybill_no=w.waybill_no and w.rn=1
 where  t.INC_DAY>='$[time(yyyyMMdd,-10d)]' 
  and   t.INC_DAY< '$[time(yyyyMMdd)]'
;



drop table if exists tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp3;
        create table tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp3  stored as parquet as
select   c.sy_hq_code as hq_code,
         c.sy_hq_name as hq_name,
         c.sy_area_code as area_code,
         c.sy_area_name as area_name,
   c.city_code as src_city,
         c.city_name as src_city_name,
         t.waybill_no,                                        --运单号    
         t.product_code,
         t.source_zone_code,                                  --原寄件网点
      t.dest_zone_code,
         t.consigned_tm,                                      --寄件时间
         t.meterage_weight_qty,                                  --计费总重量
         t.real_weight_qty,                                      --实际重量
      t.consignor_addr_native,                                --寄方客户编码
      t.quantity,                                             --包裹总件数
      t.consignee_emp_code,                                   --收件员
      t.deliver_emp_code,                                     --派件员
      t.freight_monthly_acct_code,                            --运费月结账号
         t.inputer_emp_code,
         t.signin_tm,
      t.limit_type_code,
   t.inc_day,
   t.cons_name,     -- 托寄物名
         t.pro_name,     -- 特色经济项目名称
         t.first_classify, --  一级分类
         t.second_classify, --  二级分类
         t.third_classify     --  三级分类
  from tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp2 t
left join dm_bie.bie_dim_idx_dept c on t.source_zone_code=c.dept_code
;


  insert overwrite table dm_bie.bie_spec_eco_pro_waybill_day_2022new partition (inc_day)
 select  t.waybill_no,
         t.source_zone_code,
         t.hq_code,
         t.hq_name,
         t.area_code,
         t.area_name,
   t.src_city,
         t.src_city_name,
         b.pro_name,
         b.level_1_type,
         b.level_2_type,
         b.level_3_type,
         b.pro_start,
         b.pro_end,
         t.consigned_tm,
         t.cons_name,
         FROM_UNIXTIME(UNIX_TIMESTAMP()) as load_tm,
         b.estimate_inc,
         b.inc_unit,
         t.dest_zone_code,
         t.meterage_weight_qty,
         t.real_weight_qty,
         t.consignor_addr_native,
         t.quantity,
         t.consignee_emp_code,
         t.deliver_emp_code,
         t.freight_monthly_acct_code  as  monthly_acct_code,
         t.product_code,
         t.inputer_emp_code,
         t.signin_tm,
         t.limit_type_code,
   t.first_classify, --  一级分类
         t.second_classify,
         t.third_classify,
   t.inc_day  
    
 from tmp_dm_bie.bie_spec_eco_pro_waybill_day_new_tmp3 t
inner join dm_bie.special_eco_pro_2022new b on t.area_code=b.area_code  and t.third_classify=b.LEVEL_3_TYPE
  where   to_date(t.consigned_tm)>=to_date( regexp_replace(b.pro_start,'/','-'))
   and    to_date(t.consigned_tm)<=to_date( regexp_replace(b.PRO_END,'/','-'))
;