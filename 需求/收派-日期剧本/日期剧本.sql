/**
	Q: 
		1、day_day,day_ago_date 格式化为 YYYY-MM-DD  √
		2、跨年节假日第几天/同节假日去年日期 ，
			丰景台与当前逻辑有差异。   -- 跨年的手动处理 ，  / 除夕，春节 
			-、跨年手动处理， 比如 2022-12-31（1）,2023-01-01 （2）
			-、2022-01-31 除夕
			   2023-01-21 除夕
			   2024-01-10 对应 2023-01-22 当天放假 【  2024-01-09 除夕 （不放假）】
			   
		3、	-、是否节假日当天 -- 手动调整
			也不一样（holiday_name）   -- 3倍工作日
				2024：
					元旦：1月1日三倍工资
					春节：2月10日-2月12日三倍工资
					清明节：4月4日三倍工资
					劳动节：5月1日三倍工资
					端午节：6月10日三倍工资
					中秋节：9月17日三倍工资
					国庆节：10月1日-10月3日三倍工资
			-、是否上班 -- 手动调整
		4、
			国家法定节假日 		    -- 手动填写 
			业务高峰名称			-- 手动填写  （38,618,99,1111,1212）
			产品使用高峰名称		-- 手动填写  （38,618,99,1111,1212）
		5、2026年，无核算数据。
		
		备注:
			导入数据，要保证日期格式， 没有N ，改为'' 导入 [同节假日去年日期]
		
		--、2023-12-12 导入 2026年数据 
			原条数： 14612 
			新增:	 1460 
**/

-- create table tmp_dm_predict.dm_bfms_calendar_cfg_dtl_tmp1 as
insert overwrite table tmp_dm_predict.dm_bfms_calendar_cfg_dtl_tmp1
select
  '1' as app_scanario,			-- 应用场景
  b.data_type,					-- 数据模块
  a.day_wid,					-- 日期ID 
  a.day_date,					-- 日期
  a.day_date_chn,				-- 农历日期名称 
  a.day_of_month_chn,			-- 农历本月第几天 
  a.day_of_year_chn,			-- 农历本年第几天 
  a.day_of_week,				-- 本周第几天 
  a.day_of_month,				-- 本月第几天 
  a.day_of_year,				-- 本年第几天 
  a.week_wid,					-- 周ID 
  a.quarter_wid,				-- 季度ID
  a.day_ago_date,				-- 昨日日期 
  a.is_last_day_of_week,		-- 是否周最后一天 
  a.is_last_day_of_month,		-- 是否月最后一天 
  a.is_last_day_of_year,		-- 是否年最后一天 
  a.year_ago_date_wid,			-- 去年同期日期ID 
  a.year_ago_date,				-- 去年同期日期
  a.is_weekend,					-- 是否周末
  a.is_work,					-- 是否上班
  a.public_holiday,				-- 国家法定节假日
  a.public_holiday2,			-- 节假日2
  a.is_holiday_day,				-- 是否节假日当天
  a.year_ago_holiday,			-- 同节假日去年日期
  '' as is_business_terminal,	-- 是否电商节起止日期
  a.business_peak_name,			-- 业务高峰名称
  '' as col_1,					-- 算法使用节假日名称(分析可修改)
  '' as col_2,					-- 算法使用日期(分析可修改)
  '' as col_3,					-- 时间窗口下线(分析可修改)
  '' as col_4,					-- 时间窗口上线(分析可修改)
  a.day_of_peak,				-- 业务高峰第几天
  a.day_of_holiday,				-- 节假日第几天
  '' as col_5,					-- 业务高峰趋势
  '' as col_6,					-- 业务高峰等级
  a.inc_year,					-- 年度分区
  a.work_day,					-- 核算后工作日
  a.work_day_of_month,			-- 当月累计核算后工作日
  a.work_day_of_year,			-- 当年累计核算后工作日
  '' as col_7					-- 产品使用高峰名称
from
  (
    select
      a.*,
      if(a.public_holiday <> 'N', b.day_date, 'N') as year_ago_holiday  -- 同节假日去年日期
    from
      (
        select
          day_wid,	  									-- 日期ID
          substr(day_date,1,10) as day_date,	  		-- 日期
          day_date_chn,									-- 农历日期名称
          substr(day_date_chn, 14, 2) as day_of_month_chn,	-- 农历本月第几天
          row_number() over(partition by substr(day_date_chn, 4, 4) order by day_wid) as day_of_year_chn,    -- 农历本年第几天
          day_of_week,	-- 本周第几天
          day_of_month, -- 本月第几天
          day_of_year,  -- 本年第几天
          week_wid,		-- 周ID
          quarter_wid,	-- 季度ID
          substr(day_ago_date,1,10) as day_ago_date,	 -- 昨天日期
          is_last_day_of_week,	-- 是否周最后一天(Y是，N否)
          is_last_day_of_month, -- 是否月最后一天(Y是，N否)
          is_last_day_of_year,  -- 是否年最后一天(Y是，N否)
          year_ago_date_wid,	-- 去年同期日期ID
          substr(year_ago_date,1,10) as year_ago_date,		-- 去年同期日期
          is_weekend,			-- 是否周末
          is_work,			    -- 是否上班 (按照国家假节日以及周末是否上班含补休)(Y是，N否)
          public_holiday,		-- 国家法定节假日(非法定节假日为N,法定节假日为具体节日)   --<自2019年后有具体节假日期数据>
          '' as public_holiday2,-- 重叠节假日
          if(holiday_name = 'N', 'N', 'Y') as is_holiday_day,			-- 是否节假日
          if(is_business_peak = 'N', 'N', 'Y') AS is_business_peak, 	-- 是否业务高峰(618,双十一，双十二)(Y是，N否)
          is_business_peak AS business_peak_name,						-- 业务高峰名称
          if( is_business_peak <> 'N',row_number() over(partition by inc_year,is_business_peak order by  day_wid),'N') as day_of_peak,  -- 每年/业务高峰-按日期排序
          if(public_holiday <> 'N',row_number() over(partition by inc_year,public_holiday order by day_wid),'N') as day_of_holiday,     -- 每年/法定节假日-按日期排序
          row_number() over(partition by inc_year,public_holiday order by day_date) as rank,	-- 每年/所有类型节假日-按日期排序
		  work_day,	-- 核算后工作日如2020年规则工作日1,星期六0.8,星期日0.65..
		  round(sum(work_day) over(partition by inc_year,substr(day_date,6,2) order by day_date),2)	as work_day_of_month, 	-- 当月累计核算后工作日
		  round(sum(work_day) over(partition by inc_year order by day_date),2) as work_day_of_year,  						-- 当年累计核算后工作日
          inc_year	-- 年分区
        from
          dim.dim_calendar_cfg_yi
        where
          inc_year >= '2019' 
		  and inc_year <= '2026'
      ) a
      left join (
        select
          public_holiday,
          substr(day_date,1,10) as day_date,
          inc_year,
          row_number() over(partition by inc_year,public_holiday order by day_date) as rank
        FROM
          dim.dim_calendar_cfg_yi
        where
          public_holiday <> 'N'
      ) b on (cast(a.inc_year as int) -1) = cast(b.inc_year as int)
      and a.public_holiday = b.public_holiday
      and a.rank = b.rank
  ) a
  left join (
    select
      'pickup' as data_type
    union all
    select
      'deliver' as data_type
    union all
    select
      'trans' as data_type
    union all
    select
      'flow' as data_type
  ) b on 1 = 1;
  
  
  
 /**
	
	因if(a.public_holiday <> 'N', b.day_date, 'N') = result , public_holiday不能是null，null值比较无结果， 结果直接输出为 N  
	故，若public_holiday is null  , 结果直接赋值为 'N'(实际上，null应该满足条件，应该取b.day_date)。
	解决：if( if(a.public_holiday is null ,'', a.public_holiday) <> 'N', b.day_date, 'N'),
	在结果层面：
		原：
			 'N'		4204
			null		44
		优化后：
			 'N'		2013
			null		2235
 **/

select * from (
    select
      a.*,
      a.public_holiday as public_holiday_a,
      a.rank as rank_1,
      b.public_holiday as public_holiday_b,
      b.rank as rank_2,
      b.day_date as day_date_1,
      if(a.public_holiday <> 'N', b.day_date, 'N') as year_ago_holiday,
      if(a.public_holiday <> 'N', 'A', 'B') as year_ago_holiday_T1,
      if( if(a.public_holiday is null ,'', a.public_holiday) <> 'N', 'A', 'B') as year_ago_holiday_T1_1,
      a.public_holiday <> 'N' as a_flag,
      if(a.public_holiday is null ,'', a.public_holiday) <> 'N' as a_flag_1,
      if(b.public_holiday <> 'N', b.day_date, 'N') as year_ago_holiday_2,
      if(b.public_holiday <> 'N','A', 'B') as year_ago_holiday_T2,
      if(if(b.public_holiday is null ,'', b.public_holiday)  <> 'N','A', 'B') as year_ago_holiday_T2_1,
       b.public_holiday <> 'N' as b_flag,
     if(b.public_holiday is null ,'', b.public_holiday) <> 'N' as b_flag_1
    from
      (
        select
          day_wid,	  -- 日期ID
          day_date,	  -- 日期
          day_date_chn,	-- 农历日期名称
          substr(day_date_chn, 14, 2) as day_of_month_chn,
          row_number() over(partition by substr(day_date_chn, 4, 4) order by day_wid) as day_of_year_chn,    -- 农历第几天
          day_of_week,	-- 本周第几天
          day_of_month, -- 本月第几天
          day_of_year,  -- 本年第几天
          week_wid,	-- 周ID
          quarter_wid,	-- 季度ID
          day_ago_date, -- 昨天日期
          is_last_day_of_week,	-- 是否周最后一天(Y是，N否)
          is_last_day_of_month, -- 是否月最后一天(Y是，N否)
          is_last_day_of_year,  -- 是否年最后一天(Y是，N否)
          year_ago_date_wid,	-- 去年同期日期ID
          year_ago_date,		-- 去年同期日期
          is_weekend,			-- 是否周末
          is_work,			    -- 是否上班 (按照国家假节日以及周末是否上班含补休)(Y是，N否)
          public_holiday,		-- 国家法定节假日(非法定节假日为N,法定节假日为具体节日)   --<自2019年后有具体节假日期数据>
          '' as public_holiday2,
          if(holiday_name = 'N', 'N', 'Y') as is_holiday_day,	-- 节假日
          if(is_business_peak = 'N', 'N', 'Y') AS is_business_peak,  -- 是否业务高峰(618,双十一，双十二)(Y是，N否)
          is_business_peak AS business_peak_name,					 -- 是否业务高峰(618,双十一，双十二)(Y是，N否)
          if( is_business_peak <> 'N',row_number() over(partition by inc_year,is_business_peak order by  day_wid),'N') as day_of_peak,  -- 每年/业务高峰-按日期排序
          if(public_holiday <> 'N',row_number() over(partition by inc_year,public_holiday order by day_wid),'N') as day_of_holiday,     -- 每年/法定节假日-按日期排序
          row_number() over(partition by inc_year,public_holiday order by day_date) as rank,	-- 每年/所有类型节假日-按日期排序
          inc_year	-- 年分区
        from
          dim.dim_calendar_cfg_yi
        where
          inc_year >= '2013'
          and inc_year < '2025'
      ) a
      left join (
        select
          public_holiday,
          day_date,
          inc_year,
          row_number() over(partition by inc_year,public_holiday order by day_date) as rank
        FROM
          dim.dim_calendar_cfg_yi
        where
          public_holiday <> 'N'
      ) b on (cast(a.inc_year as int) -1) = cast(b.inc_year as int)
      and a.public_holiday = b.public_holiday
      and a.rank = b.rank
) al where al.day_wid in ( '20190209' , '20160706')