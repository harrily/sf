-- 487936	 【中转场OD流向】动态

	-- 487924  OD动态-运力打点
	输入： 
		super_flow_vehicle_tasks_split_info_full_hudi_pro_ro   -- 运力准实时数据表（车辆拆经停）  运力类型(0 车辆,1 全货机,2 散航班, 3铁路,6丰湃)
			-- 取运力表线路运行时间T-4D~T+0D
			-- 实际到达班次日期T-1D~T+0D的数据
		dim.dim_department  
			-- 约束网点类型
	输出： 
		dm_ordi_predict.dwd_cvy_actual_arrive_batch_contnrcode_dtl_hf_v2 (inc_day='$[time(yyyyMMddHHmm)]')
		
	输入：	
		dm_ordi_predict.dim_city_trans_mapping_multi_eta_wf  -- 城市中转场件量映射一对多
	输出： 
		tmp_ordi_predict.tmp_gtl_city_trans_mapping_multi  -- 取最新分区数据。
	
	-- 487918  OD动态-装车运单去重
	输入： dm_ordi_predict.dwd_load_waybill_prd_dtl_rt		-- prd全网运单宽表实时kafka2hive
			-- 装车运单需要取T-4~T-0的装车运单，
			-- 考虑落数延迟，分区取T-5~T-0
	输出： tmp_ordi_predict.tmp_gtl_dwd_load_waybill_dtl_di
	
	
		-- 487934  OD动态-关联
		输入：dm_ordi_predict.dwd_cvy_actual_arrive_batch_contnrcode_dtl_hf_v2  L+  tmp_ordi_predict.tmp_gtl_dwd_load_waybill_dtl_di
			-- 车标关联 
		输出： tmp_ordi_predict.tmp_gtl_dwd_od_cvy_waybill_dtl_di
	
			-- 487935  OD动态-汇总 
			输入： tmp_ordi_predict.tmp_gtl_dwd_od_cvy_waybill_dtl_di  L+ dwd_o.dwd_st_batch_info_di （中转班次基表）
				-- 1、主表 聚合计算每个运力运力，票量/计费重量  
				-- 2、按照 实际到达班次时间/目的网点/实际到达班次 关联   取主表，辅表 集散类型/新班次编码/班次开始时间s/班次结束时间s/最晚到达时间/
			输出： tmp_ordi_predict.tmp_gtl_arrive_batch_to_dest_trans_dynamic_dtl 
			
			-- 按照城市中转场一对多比例关系映射
			输入： tmp_ordi_predict.tmp_gtl_arrive_batch_to_dest_trans_dynamic_dtl  L+ tmp_ordi_predict.tmp_gtl_city_trans_mapping_multi
				-- 根据目的城市关联 ，取辅表 目的中转场，（城市中转场件量）比率 ， 根据比率计算对应指标值
			输出：  dm_ordi_predict.dws_arrive_batch_to_dest_trans_dynamic_dtl_hf_v2  [最近数据=202302281705]
				