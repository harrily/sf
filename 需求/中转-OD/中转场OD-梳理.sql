/**
任务依赖概图：
466176 		  ->	487391 [spark-3,hive-1,-- T-14D~T-1D]
134963/576764 ->	466406 [spark]
167471		  ->	466415 [spark,-- T-14D~T-1D]	->	466997 - 953811 [hive]  -> 467012 [hive]
247889		  ->	466421 [spark,-- T-14D~T-1D]

466997[hive]
	953811 - spark 
467012 -- 去掉自依赖。


1、日期 	
	v1 - 写入自然日作为分区
		-- 班次日期- 近7天 -- 来源pass[中转班次预测预警]（班次日期含义- 和聪哥沟通）
		
	OD - 到达日期作为分区
		- 运单实际到达时间 + 巴枪约束 -- 到达日期。

	-、fvp-过滤：
		571 - 铁路到车
		106 - 航空落地
		303	- 到达经停点
	-、运单路由信息表
		-、
			first_tm_305  -- 到车时间(巴枪扫描时间-兜底)
			first_tm_31   -- 卸车时间
		-、到达运单类型
		-、
			actual_arrive_batch  到达_运力实际到达班次 
			actual_arrive_tm     到达_运力实际到车时间
			
	差异原因：
		1.统计时间不一样
			v1底盘 - 近7D班次时间写入当前分区
			OD- 根据到达时间，写入当前分区
		2.口径不一致
			v1底盘 - 卸车
			OD - 到车+卸车
			后面还需要再往上运单级别看下，作证下原因
		3、inc_day 范围滑动导致（2023-02月， inc_day 扩大范围，未过滤票量会有明显变化）。

dm_ordi_predict.dwd_waybill_dynimic_dest_trans_dtl_di  		-- 487391  -- 运单路由信息 （到车/卸车 -时间）
dm_ordi_predict.dim_trans_first_last_batch_info_df     		-- 466406  -- 中转班次基表 （操作网点/班次编码/	班次开始时间/最晚到达时间/班次结束时间   -- 网点-最早/最晚）
dm_ordi_predict.dwd_waybill_pis_dest_trans_dtl_di 			-- 466415  -- 静态-运单pis静态-目的中转场（from ods_pis.tt_waybill_route_info-pis运单路由信息）
																		1-、中转场降序 + 路由序号降序  --> 目的中转场 
dm_ordi_predict.dwd_waybill_split_with_dest_trans_dtl_di 	-- 466421  -- 运单宽表-拆分子单重量及目的中转场	 
																		1-、运单宽表  -- 目的网点 ，拆分子单 
																		2-、网点-中转场-映射关系表 -
														
补录： 当前数据截止 2023-02-25 
	2023-12-25
	2023-12-12
	2023-11-28
	2023-11-14 
	
tmp_ordi_predict.tmp_yc_fvp_dtl_di_v1_history_flush
tmp_ordi_predict.tmp_dwd_waybill_dynimic_dest_trans_dtl_di_v1_historyh_flush

补数备注：
	1-、flow ID : 955215
	2、466415任务：
		-、表替换新表： 
			ods_pis.tt_waybill_route_info  -- 替换为新表 ： dwd_o.dwd_plan_dynamic_static_waybill_route_di 
			后续旧表也申请。
	3、补数时间点
		2023-03-12  
		2023-03-26
		2023-04-09
		2023-04-23
		2023-05-07
		2023-05-21
		2023-06-04
		2023-06-18
		2023-07-02
		2023-07-16
		2023-07-30
		2023-08-13
		2023-08-27
		2023-09-10
		2023-09-24
		2023-10-08
		2023-10-22
		2023-11-05
		2023-11-19
	
	备注： 
		补数由远及近补录(因为寄件取近21天，到达近14天)。

1. 到件还是发件	 
	-- 到件
2. 起始和目的中转场的定义是什么
	-- 根据运单路由信息表（途径中转环节排名）
3. 是否都是二级中转场
	-- 查看上游表近1年，中转场级别都为null.

**/
-- 466406 [spark] -- dm_ordi_predict.dim_trans_first_last_batch_info_df

/**
	-- 1、487391  【中转场OD流向】静态-运单动态路由中转场
		1-- 核心代码-注释
		2-- 数据表-血缘关系
			dwd.dwd_waybill_route_info_di [20200101~至今]		  	-> dm_ordi_predict.dwd_waybill_cvy_type_dtl_di
			dwd.dwd_waybill_route_info_di [20200101~至今]			-> tmp_ordi_predict.tmp_dwd_waybill_dynimic_dest_trans_dtl_di
			ods_kafka_fvp.fvp_core_fact_route_op  [20161127~至今]	-> tmp_ordi_predict.tmp_yc_fvp_dtl_di
	
**/
insert overwrite table dm_ordi_predict.dwd_waybill_dynimic_dest_trans_dtl_di partition(inc_day)
select 
    a.waybill_no,					-- 运单号
    dept_code,						-- 路由网点代码
    dept_name,						-- 路由网点名称
    is_freight,						-- 是否快运中转场
    dept_type_code,					-- 网点类型代码
    dept_type_name,					-- 网点类型名称
    dept_level,						-- 中转场级别
    city_code,						-- 城市代码
    city_name,						-- 城市名称
    province_name,					-- 省份名称
    step_type,						-- 收_中转_派类型
    rank_no,						-- 途径中转环节排名
    plan_arrive_batch,				-- 到达_运力计划到达班次
    actual_arrive_batch,			-- 到达_运力实际到达班次
    plan_arrive_tm,					-- 到达_运力计划到车时间
    actual_arrive_tm,				-- 到达_运力实际到车时间
    plan_send_batch,				-- 发出_运力计划发出班次
    actual_send_batch,				-- 发出_运力实际发出班次
    plan_depart_tm,					-- 发出_运力计划发车时间
    actual_depart_tm,				-- 发出_运力实际发车时间
    rn,								-- 按运单rank_no倒叙后排序名次
    arrive_line_require_id,			-- 到达_计划需求id
    leave_line_require_id,			-- 发出_计划需求id
    arrive_cvy_type,				-- 到达_运力类型
    depart_cvy_type,				-- 发出_运力类型
    b.arrive_waybill_type,  		-- 到达运单类型(全货机,散航,铁路,干支线) [根据到达运力类型判断]
    first_tm_305,					-- 首次到车时间
	first_tm_31,					-- 首次卸车时间
    inc_day							-- 分区
from 
	(	
		select
			*
		from 
			tmp_ordi_predict.tmp_dwd_waybill_dynimic_dest_trans_dtl_di
		where nvl(first_tm_305,'')!=''	-- -- 首次到车时间-不为null
		union all
		select
			m.waybill_no,
			m.dept_code,
			m.dept_name,
			m.is_freight,
			m.dept_type_code,
			m.dept_type_name,
			m.dept_level,
			m.city_code,
			m.city_name,
			m.province_name,
			m.step_type,
			m.rank_no,
			m.plan_arrive_batch,
			m.actual_arrive_batch,
			m.plan_arrive_tm,
			m.actual_arrive_tm,
			m.plan_send_batch,
			m.actual_send_batch,
			m.plan_depart_tm,
			m.actual_depart_tm,
			m.rn,
			m.arrive_line_require_id,
			m.leave_line_require_id,
			m.arrive_cvy_type,
			m.depart_cvy_type,
			n.barscantm as first_tm_305,  -- 巴枪(最后)扫描时间
			m.first_tm_31,
			m.inc_day
		FROM
			(
				select * 
				from tmp_ordi_predict.tmp_dwd_waybill_dynimic_dest_trans_dtl_di   -- 通过网点类型代码筛选中转场的路由
				where nvl(first_tm_305,'')=''		-- 首次到车时间-为null
			) m
		left join
			tmp_ordi_predict.tmp_yc_fvp_dtl_di n	-- 巴枪路由信息表
		on m.waybill_no=n.mainwaybillno and m.dept_code=n.zonecode
	) a
left join (
    select 
        waybill_no,
        arrive_waybill_type 
    from dm_ordi_predict.dwd_waybill_cvy_type_dtl_di   -- 根据运力类型计算运单类型 
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
) b
on a.waybill_no=b.waybill_no
;



-- 2、 466997  【中转场OD流向】静态-目的中转场多表关联明细
	-- 核心代码-注释
insert overwrite table dm_ordi_predict.dwd_waybill_dest_trans_dtl_di partition(inc_day)
select 
    a.waybill_no,																	-- 运单号
    a.dept_code,																	-- 路由网点代码
    a.dept_name,																	-- 路由网点名称
    a.is_freight,																	-- 是否快运中转场
    a.dept_type_code,																-- 网点类型代码
    a.dept_type_name,																-- 网点类型名称
    a.dept_level,																	-- 中转场级别
    a.city_code,																	-- 城市代码
    a.city_name,																	-- 城市名称
    a.province_name,																-- 省份名称
    a.step_type,																	-- 收_中转_派类型
    a.rank_no,																		-- 途径中转环节排名
    a.plan_arrive_batch,															-- 到达_运力计划到达班次
	nvl(a.actual_arrive_batch,m.trans_batch_code) as actual_arrive_batch,			-- 到达_运力实际到达班次
    a.plan_arrive_tm,																-- 到达_运力计划到车时间
    a.actual_arrive_tm,																-- 到达_运力实际到车时间
    a.plan_send_batch,																-- 发出_运力计划发出班次
    a.actual_send_batch,															-- 发出_运力实际发出班次
    a.plan_depart_tm,																-- 发出_运力计划发车时间
    a.actual_depart_tm,																-- 发出_运力实际发车时间
    a.inc_day as dynamic_inc_day,													-- 动态路由表分区
    --b.waybill_no,
    b.dept_code as dynamic_dest_trans,												-- 路由网点代码 -- 最后一个环节中转场
    --b.inc_day,
    c.main_waybill_no,																-- 运单号
    c.source_zone_code,																-- 原寄地网点代码
    c.addressee_dept_code,															-- 目的地网点代码
    c.dest_city_code,																-- 目的地城市代码
    c.meterage_weight_qty,															-- 计费总重量
    c.meterage_weight_qty_kg,														-- 计费总重量（kg）
    c.consigned_tm,																	-- 寄件时间
    c.signin_tm,																	-- 签收时间
    c.package_no,																	-- 包裹运单号
    --c.waybill_no,
    c.split_meterage_weight_qty,													-- 平均拆分计费总重量
    c.split_meterage_weight_qty_kg,													-- 平均拆分计费总重量（kg）
    c.is_signin,																	-- 是否签收
    c.waybill_dest_trans,															-- 运单目的中转场
    c.inc_day as consigned_dt,														-- 运单宽表分区
    --d.waybill_no,
    d.s_zone_code as pis_dest_trans,												-- pis静态目的中转场
    --d.inc_day
    (case 
        when nvl(nvl(a.actual_arrive_tm,a.first_tm_305),a.first_tm_31) < t.first_start_tm and nvl(a.actual_arrive_batch,m.trans_batch_code)!=t.first_batch_code then date_add(to_date(nvl(nvl(a.actual_arrive_tm,a.first_tm_305),a.first_tm_31)),-1)
        when nvl(nvl(a.actual_arrive_tm,a.first_tm_305),a.first_tm_31) > t.last_start_tm and nvl(a.actual_arrive_batch,m.trans_batch_code)!=t.last_batch_code then date_add(to_date(nvl(nvl(a.actual_arrive_tm,a.first_tm_305),a.first_tm_31)),+1)
        else to_date(nvl(nvl(a.actual_arrive_tm,a.first_tm_305),a.first_tm_31))
    end) as actual_arrive_batch_date,												-- 实际到达班次日期
    (case 
        when c.is_signin=1 then b.dept_code 
        when b.dept_code != d.s_zone_code and !array_contains(c.waybill_dest_trans,b.dept_code) then d.s_zone_code
        else b.dept_code 
    end) as final_dest_trans,														-- 最终目的中转场
    --a.col_01 as col_01,															-- arrive_line_require_id 到车计划需求id --20220819
    --a.col_02 as col_02,															-- leave_line_require_id	发车计划需求id --20220819
    a.arrive_line_require_id as col_01,												-- arrive_line_require_id 到车计划需求id --20220819
    a.leave_line_require_id as col_02,												-- leave_line_require_id	发车计划需求id --20220819  
    nvl(t2.transit_code,c.dest_city_code) as col_03,								-- 20221014 如果城市映射不到中转场则取目的城市-国外城市
    '' as col_04,
    '' as col_05,
    a.arrive_cvy_type,															    -- 到达_运力类型
    a.depart_cvy_type, 																-- 发出_运力类型
    a.arrive_waybill_type,															-- 运单类型
    c.limit_type_code, 																-- 时效类型代码
    c.limit_type_name, 																-- 时效类型分类
    c.product_code,  																-- 产品代码
    c.product_name, 																-- 产品名称
    c.inc_day 																		-- 按照寄件时间分区
from (
    select 
		waybill_no,						-- 运单号
		dept_code,						-- 路由网点代码
		dept_name,						-- 路由网点名称
		is_freight,						-- 是否快运中转场
		dept_type_code,					-- 网点类型代码
		dept_type_name,					-- 网点类型名称
		dept_level,						-- 中转场级别
		city_code,						-- 城市代码
		city_name,						-- 城市名称
		province_name,					-- 省份名称
		step_type,						-- 收_中转_派类型
		rank_no,						-- 途径中转环节排名
		plan_arrive_batch,				-- 到达_运力计划到达班次
		actual_arrive_batch,			-- 到达_运力实际到达班次
		plan_arrive_tm,					-- 到达_运力计划到车时间
		actual_arrive_tm,				-- 到达_运力实际到车时间
		plan_send_batch,				-- 发出_运力计划发出班次
		actual_send_batch,				-- 发出_运力实际发出班次
		plan_depart_tm,					-- 发出_运力计划发车时间
		actual_depart_tm,				-- 发出_运力实际发车时间
        arrive_line_require_id,			-- arrive_line_require_id 到车计划需求id --20220819
        leave_line_require_id, 			-- leave_line_require_id	发车计划需求id --20220819
        arrive_cvy_type, 				-- 到达_运力类型
        depart_cvy_type, 				-- 发出_运力类型
        arrive_waybill_type,			-- 运单类型
		first_tm_305,					-- 首次到车时间
		first_tm_31,					-- 首次卸车时间
        inc_day							-- 分区
    from dm_ordi_predict.dwd_waybill_dynimic_dest_trans_dtl_di
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
) a
left join (
    select 
        waybill_no,						-- 运单号
        dept_code,						-- 路由网点代码
        inc_day
    from dm_ordi_predict.dwd_waybill_dynimic_dest_trans_dtl_di
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
    and rn = 1   						-- 取途径中转环节的最后一个
) b
on a.waybill_no=b.waybill_no
left join (
    select 
        main_waybill_no,				--运单号
        source_zone_code,				--原寄地网点代码
        addressee_dept_code,			--目的地网点代码
        dest_city_code,					--目的地城市代码
        meterage_weight_qty,			--计费总重量
        meterage_weight_qty_kg,			--计费总重量（kg）
        consigned_tm,					--寄件时间
        signin_tm,						--签收时间
        package_no,						--包裹运单号
        waybill_no,						-- 子运单号
        split_meterage_weight_qty,		-- 平均拆分计费总重量
        split_meterage_weight_qty_kg,	-- 平均拆分计费总重量（kg）
        is_signin,						--是否签收
        waybill_dest_trans,				-- 运单目的中转场
        limit_type_code, 				--时效类型代码
        limit_type_name,  				--时效类型分类
        product_code,  					--产品代码
        product_name, 					--产品名称
        inc_day 				
    from dm_ordi_predict.dwd_waybill_split_with_dest_trans_dtl_di		-- （运单(子母件)目的中转场明细表
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
) c
on a.waybill_no=c.waybill_no
left join (
    select 
        waybill_no,						-- 运单号
        s_zone_code,					-- 静态网点
        inc_day
    from dm_ordi_predict.dwd_waybill_pis_dest_trans_dtl_di				-- 运单pis静态目的中转场明细表
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
    and rn = 1
) d 
on a.waybill_no=d.waybill_no
left join (
    select
        dept_code,						-- 操作网点
        first_batch_code,				-- 最早班次代码
        first_start_tm,					-- 最早班次计划开始时间
        first_arrv_tm,					-- 最早班次最晚到达时间
        first_end_tm,					-- 最早班次计划结束时间
        last_batch_code,				-- 最晚班次代码
        last_start_tm,					-- 最晚班次计划开始时间
        last_arrv_tm,					-- 最晚班次最晚到达时间
        last_end_tm,					-- 最晚班次计划结束时间
        inc_day
    from dm_ordi_predict.dim_trans_first_last_batch_info_df a			-- 中转场最早最晚班次维度表
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
) t
on replace(to_date(a.actual_arrive_tm),'-','')=t.inc_day 
and a.dept_code=t.dept_code
left join (
	select 
		city_code,						-- 城市代码
		transit_code  					-- 中转场代码
	from dm_ordi_predict.dim_city_trans_mapping_eta_dtl_df				-- 不同路由代码城市与代表网点的映射关系
	where inc_day='$[time(yyyyMMdd,-2d)]'
) t2
on c.dest_city_code=t2.city_code
left join (
    select
      trans_batch_code,					-- 班次编码
      dept_code,						-- 操作网点
      plan_start_tm,					-- 计划开始时间
      last_arrv_tm,						-- 最晚到达时间
      plan_end_tm,						-- 规定结束时间
      last_plan_end_time,				-- 上一班次结束时间
      front_last_arrv_tm,				-- 上一班次最晚到达时间
      next_start_tm						-- 下一班次开始时间
    from
      dim.dim_trans_batch_info_df										-- 中转班次维表
    where
      inc_day>='$[time(yyyyMMdd,-16d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
    group by
      trans_batch_code,
      dept_code,
      plan_start_tm,
      last_arrv_tm,
      plan_end_tm,
      last_plan_end_time,
      front_last_arrv_tm,
      next_start_tm
  ) m 
on a.dept_code = m.dept_code
where
  nvl(a.first_tm_305, a.first_tm_31) >= m.front_last_arrv_tm
  and nvl(a.first_tm_305, a.first_tm_31) <= m.last_arrv_tm;
;


-- 3、 467012  【中转场OD流向】静态-汇总结果
	-- 核心代码-注释
	
/**
		数据核验：
			actual_arrive_batch_date
			quantity_cnt 
			quantity_distinct_cnt

		-- 核验： 
			select inc_day,
			sum(quantity_cnt) as  quantity_cnt,
			sum(quantity_distinct_cnt) as quantity_distinct_cnt,
			sum(quantity_cnt_v1) as  quantity_cnt_v1,
			sum(quantity_cnt_v2) as  quantity_cnt_v2,
			sum(quantity_cnt_v3) as  quantity_cnt_v3,
			sum(quantity_cnt_v4) as  quantity_cnt_v4,
			-- sum(quantity_cnt_v5) as  quantity_cnt_v5,
			sum(quantity_cnt_v6) as  quantity_cnt_v6
			 from 
			(
				select 
					actual_arrive_batch_date,									--实际到达班次日期
					dept_code,													--网点代码
					--dept_name,												--网点名称
					--is_freight,												--是否快运
					--dept_type_code,											--网点类型代码
					--dept_type_name,											--网点类型名称
					--dept_level,												--中转场级别
					actual_arrive_batch,										--实际到达班次
					final_dest_trans,											--最终目的中转场
					count(waybill_no) as quantity_cnt,							--件数
					count(distinct waybill_no) as quantity_distinct_cnt,		--件数distinct
					sum(split_meterage_weight_qty) as meterage_weight_qty,		--计费重量
					sum(split_meterage_weight_qty_kg) as meterage_weight_qty_kg,--计费重量kg 
					replace(actual_arrive_batch_date,'-','') as inc_day,
					 
					if(nvl(actual_arrive_batch_date,'')!='', count(waybill_no),0) as quantity_cnt_v1,
					if(nvl(actual_arrive_batch,'')!='', count(waybill_no),0) as quantity_cnt_v2,
					if(nvl(final_dest_trans,'')!='', count(waybill_no),0) as quantity_cnt_v3,
					if(nvl(cast(final_dest_trans as int),'')='', count(waybill_no),0) as quantity_cnt_v4,
					-- if(dept_code != pis_dest_trans and !array_contains(waybill_dest_trans,dept_code), count(waybill_no),0) as quantity_cnt_v5,
					if(dept_code != final_dest_trans, count(waybill_no),0) as quantity_cnt_v6

				from dm_ordi_predict.dwd_waybill_dest_trans_dtl_di
					where inc_day>='$[time(yyyyMMdd,-21d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
			-- and nvl(actual_arrive_batch_date,'')!=''												--实际到达班次日期为空不统计
			-- and nvl(actual_arrive_batch,'')!='' 													--实际到达班次为空不统计
			-- and nvl(final_dest_trans,'')!='' 													--目的中转场为空不统计
			-- and nvl(cast(final_dest_trans as int),'')=''											--目的地中转场为城市不统计
			-- and dept_code != pis_dest_trans and !array_contains(waybill_dest_trans,dept_code) 	--当前中转场不是pis目的中转场，也不是运单映射目的中转场的才统计
			-- and dept_code != final_dest_trans                                                    --起始网点不是同一网点
					and actual_arrive_batch_date>='$[time(yyyy-MM-dd,-14d)]' and actual_arrive_batch_date<='$[time(yyyy-MM-dd,-1d)]'
					group by  actual_arrive_batch_date,														--实际到达班次日期
						dept_code,																			--网点代码
						--dept_name,																		--网点名称
						--is_freight,																		--是否快运
						--dept_type_code,																	--网点类型代码
						--dept_type_name,																	--网点类型名称
						--dept_level,																		--中转场级别
						actual_arrive_batch,																--实际到达班次
						final_dest_trans			
			)al group by inc_day
**/
insert overwrite table dm_ordi_predict.dws_arrive_batch_to_dest_trans_dtl_di partition(inc_day)
select 
    actual_arrive_batch_date,						--实际到达班次日期
    a.dept_code,									--网点代码
    '' as dept_name,								--网点名称
    '' as is_freight,								--是否快运
    '' as dept_type_code,							--网点类型代码
    '' as dept_type_name,							--网点类型名称
    '' as dept_level,								--中转场级别
    actual_arrive_batch,							--实际到达班次
    final_dest_trans,								--最终目的中转场
    quantity_cnt,									--件数
    quantity_distinct_cnt,							--件数distinct
    meterage_weight_qty,							--计费重量
    meterage_weight_qty_kg,							--计费重量kg
    b.new_batch_code as new_batch_code,				-- 新班次编码
    b.plan_start_tm as col_02,						-- 计划开始时间
	b.plan_end_tm as col_03,						-- 规定结束时间
	b.last_arrv_tm as col_04,						-- 最晚到达时间
    '' as col_05,    
    replace(actual_arrive_batch_date,'-','') as inc_day
from (
    select 
        actual_arrive_batch_date,									--实际到达班次日期
        dept_code,													--网点代码
        --dept_name,												--网点名称
        --is_freight,												--是否快运
        --dept_type_code,											--网点类型代码
        --dept_type_name,											--网点类型名称
        --dept_level,												--中转场级别
        actual_arrive_batch,										--实际到达班次
        final_dest_trans,											--最终目的中转场
        count(waybill_no) as quantity_cnt,							--件数
        count(distinct waybill_no) as quantity_distinct_cnt,		--件数distinct
        sum(split_meterage_weight_qty) as meterage_weight_qty,		--计费重量
        sum(split_meterage_weight_qty_kg) as meterage_weight_qty_kg,--计费重量kg 
        replace(actual_arrive_batch_date,'-','') as inc_day
    from dm_ordi_predict.dwd_waybill_dest_trans_dtl_di
		where inc_day>='$[time(yyyyMMdd,-21d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
		--and nvl(col_01,'')!='' 																--arrive_line_require_id 到车计划需求id --20220819 为空代表非车辆运力 撤回
		and nvl(actual_arrive_batch_date,'')!=''												--实际到达班次日期为空不统计
		and nvl(actual_arrive_batch,'')!='' 													--实际到达班次为空不统计
		and nvl(final_dest_trans,'')!='' 														--目的中转场为空不统计
		and nvl(cast(final_dest_trans as int),'')=''											--目的地中转场为城市不统计
		--and is_signin=0 																		--已签收不统计 不能加该条件 因为历史数据最终都是签收了
		and dept_code != pis_dest_trans and !array_contains(waybill_dest_trans,dept_code) 		--当前中转场不是pis目的中转场，也不是运单映射目的中转场的才统计
		and dept_code != final_dest_trans
		and actual_arrive_batch_date>='$[time(yyyy-MM-dd,-14d)]' and actual_arrive_batch_date<='$[time(yyyy-MM-dd,-1d)]'
		group by  actual_arrive_batch_date,														--实际到达班次日期
			dept_code,																			--网点代码
			--dept_name,																		--网点名称
			--is_freight,																		--是否快运
			--dept_type_code,																	--网点类型代码
			--dept_type_name,																	--网点类型名称
			--dept_level,																		--中转场级别
			actual_arrive_batch,																--实际到达班次
			final_dest_trans																	--最终目的中转场
) a 
left join (
    select 
        trans_batch_code,			-- 班次编码
        new_batch_code,				-- 新班次编码
        dept_code,					-- 操作网点
        inc_day,					-- 分区
		plan_start_tm,				-- 计划开始时间
		plan_end_tm,				-- 规定结束时间
		last_arrv_tm				-- 最晚到达时间
    from dim.dim_trans_batch_info_df				-- 中转班次维表
    where inc_day>='$[time(yyyyMMdd,-14d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
) b
on replace(a.actual_arrive_batch_date,'-','')=b.inc_day
and a.dept_code=b.dept_code
and a.actual_arrive_batch=b.trans_batch_code
;



/**

	验数： 
	
	-- 历史中转场类别 计数
select inc_day,dept_level, count(1)
from dm_ordi_predict.dwd_waybill_dynimic_dest_trans_dtl_di
where inc_day >= '20220101'
group by inc_day,dept_level
-- 中转场类别 来源
select distinct dept_level
from dwd.dwd_waybill_route_info_di
where inc_day>='$[time(yyyyMMdd,-300d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
and dept_type_code in ('ZZC04-TCJS', --同城集散点
                'ZZC04-TL', --铁路站点
                'ZZC04-YJ', --片区中转场
                'ZZC04-SN', --陆运枢纽
                'ZZC04-LS', --临时中转场
                'ZZC04-ERJ', --快运中转场
                'ZZC05-SJ', --集散点
                'DB05-HHWZ', --货航外站
                'ZZC04-HKHZ', --航空站点
                'ZZC04-HK', --航空枢纽
                'GWB04', --关务站点
                'ZZC04-JYHK', --简易航空站点
                'ZZC05-KYJS') --快运集散点

--  '036340466361','058286666534'
select 
    waybill_no,
    dept_code,
    dept_name,
    is_freight,
    dept_type_code,
    dept_type_name,
    dept_level,
    city_code,
    city_name,
    province_name,
    step_type,
    rank_no,
    plan_arrive_batch,
    actual_arrive_batch,
    plan_arrive_tm,
    actual_arrive_tm,
    plan_send_batch,
    actual_send_batch,
    plan_depart_tm,
    actual_depart_tm,
    row_number() over(partition by waybill_no order by rank_no desc) as rn,
    arrive_line_require_id,--arrive_line_require_id 到车计划需求id --20220819
    leave_line_require_id,--leave_line_require_id	发车计划需求id --20220819
    arrive_cvy_type, --到达_运力类型
    depart_cvy_type, --发出_运力类型
    first_tm_305,
	first_tm_31,
    inc_day
from dwd.dwd_waybill_route_info_di
where inc_day>='$[time(yyyyMMdd,-5d)]' and inc_day<='$[time(yyyyMMdd,-1d)]'
and waybill_no in ('036340466361','058286666534')

-- '038633134212','069490419375'
select * from 
dm_ordi_predict.dwd_waybill_dynimic_dest_trans_dtl_di
where inc_day = '20230204'
and waybill_no in ('038633134212','069490419375')

select count(1) from 
dm_ordi_predict.dwd_waybill_dest_trans_dtl_di
where 
 inc_day = '20230204'
-- and waybill_no in ('038633134212','069490419375')
and actual_arrive_batch_date = '2023-02-04'
and dept_code in ('757WA')
and final_dest_trans = '510WF'
and actual_arrive_batch = '757WA2025'
and nvl(actual_arrive_batch_date,'')!=''													--实际到达班次日期为空不统计
    and nvl(actual_arrive_batch,'')!='' 													--实际到达班次为空不统计
    and nvl(final_dest_trans,'')!='' 														--目的中转场为空不统计
    and nvl(cast(final_dest_trans as int),'')=''											--目的地中转场为城市不统计
    --and is_signin=0 																		--已签收不统计 不能加该条件 因为历史数据最终都是签收了
    and dept_code != pis_dest_trans and !array_contains(waybill_dest_trans,dept_code) 		--当前中转场不是pis目的中转场，也不是运单映射目的中转场的才统计
    and dept_code != final_dest_trans


select * from 
dm_ordi_predict.dws_arrive_batch_to_dest_trans_dtl_di
where 
-- inc_day = '20230204'
actual_arrive_batch_date = '2023-02-04'
and dept_code in ('757WA')
and final_dest_trans = '510WF'
and actual_arrive_batch = '757WA2025'

**/