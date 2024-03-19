
-- step3  -- pis发件-获取运力信息
	-- 约束历史小时时点。
set runner.support.reading-hudi=true;

drop table if exists tmp_ordi_predict.tmp_send_flow_static_pis_05_1_tmp20240111_v1;
create table if not exists tmp_ordi_predict.tmp_send_flow_static_pis_05_1_tmp20240111_v1 
stored as parquet as 
select 
        line_code
        ,plan_send_batch
        ,src_zone_code
        ,dest_zone_code
        ,cast(datediff(plan_arrive_dt,plan_send_batch_dt) as int) as dt_period
    from  
        (select line_code
            ,plan_send_batch
            ,src_zone_code
            ,dest_zone_code
            ,plan_send_batch_dt
            ,to_date(plan_arrive_tm) as plan_arrive_dt
            ,plan_arrive_batch_dt
            ,row_number()over(partition by line_code,src_zone_code order by create_tm desc) as rn 
        from  dm_oia.super_flow_vehicle_tasks_split_info_full_hudi_pro_rt a
        where 
		 line_require_date>='$[time(yyyy-MM-dd,-30d)]' and line_require_date<='$[time(yyyy-MM-dd,+7d)]'
		-- line_require_date BETWEEN '2023-12-12' and '2024-01-18'					--  回刷历史，约束时间范围 
		and last_update_tm  <=  '$[time(yyyy-MM-dd HH:00:00)]'						--  回刷历史，约束时间范围  last_update_tm	2022-01-02 09:07:06.068
		-- 	and create_tm  <= '2024-01-11 12:00:00'									--  回刷历史，约束时间范围  create_tm	2021-12-25 06:14:47.743
        ) t
        where t.rn=1 and line_code is not null 
    group by  line_code
        ,plan_send_batch
        ,src_zone_code
        ,dest_zone_code
        ,cast(datediff(plan_arrive_dt,plan_send_batch_dt) as int);
