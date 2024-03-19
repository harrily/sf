drop table if exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp3;
create table if not exists tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp3
stored as parquet as
select 
       t.cons_name_concat,
       fetch_data,
       cast(c.priority_no as int)   priority_no,            --优先级
       c.broad_heading,                                     --大类
       subdivision                                          --小类
--   from  dm_bie.tm_consign_info c
from  dm_ordi_predict.tm_consign_info c   -- 20230906 使用复制的托寄物配置表
  join tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2 t
  -- join dm_bie.tm_consign_info c 
  where  c.fetch_type = '包含' 
  and instr(t.cons_name_concat_up,upper(c.fetch_data)) > 0
  union all 
  select 
       t.cons_name_concat,
       fetch_data,
       cast(c.priority_no as int)   priority_no,            --优先级
       c.broad_heading,                                     --大类
       subdivision                                          --小类
--   from dm_bie.tm_consign_info c 
from  dm_ordi_predict.tm_consign_info c   -- 20230906 使用复制的托寄物配置表
  join 
  tmp_ordi_predict.tmp_shenhy_special_eco_waybill_ref_tmp2  t
  -- join dm_bie.tm_consign_info c 
  on  c.fetch_type = '等于' 
  and cons_name_concat_up = upper(c.fetch_data);