package config;

public class GlobalSql {
	
	// 根据 表owner + 表名  + 列名 获取列是否存在
	public static final String CHK_TAB_COLUMN_EXISTS_SQL
	 	= "select count(1) AS TOTAL_COL_ROW_COUNT" +
	 	  "  from information_schema.COLUMNS col " + 
	      " where col.TABLE_SCHEMA = ? " + 
	 	  "   and col.TABLE_NAME = ? " + 
	 	  "   and col.COLUMN_NAME = ? " + 
	 	  " LIMIT 1 ";
	
	// 维护了规则: 根据 source_sys_key + table_owner + table_name 获取存在规则的列信息
	public static final String QUERY_SOUR_TAB_COLUMNS_IN_RULES
		= "select drh.SOURCE_SYS_KEY, " + 
		  "       drh.SOURCE_TABLE_OWNER, " + 
		  "       drh.SOURCE_TABLE_NAME, " + 
		  "       drh.DEST_TABLE_OWNER, " + 
		  "       drh.DEST_TABLE_NAME, " + 
		  "       ifnull(drh.FROM_CONDITION_TAB_LIST, CONCAT(drh.SOURCE_TABLE_OWNER, '.', drh.SOURCE_TABLE_NAME)) FROM_CONDITION_TAB_LIST, " + 
		  "       drh.WHERE_CONDITION_SQL, " + 
		  "       drl.SOURCE_TAB_COLUMN_NAME, " + 
		  "       ifnull(drl.DP_RETURN_COLUMN_VAL, " + 
		  "              ifnull(get_dp_rules_field_map_case_exp(drh.SOURCE_SYS_KEY, drh.SOURCE_TABLE_OWNER, drh.SOURCE_TABLE_NAME, drl.SOURCE_TAB_COLUMN_NAME, drl.DP_RULES_LINE_ID), " + 
		  "                     CONCAT('" + GlobalInfo.SOUR_TAB_ALIAS_NAME + "', '.', drl.SOURCE_TAB_COLUMN_NAME))) DP_RETURN_COLUMN_VAL, " + 
		  "       drl.DEST_TAB_COLUMN_NAME, " + 
		  "       col.COLUMN_NAME " + 
		  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all drh, " + 
		  "       " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_lines_all   drl, " + 
		  "       information_schema.COLUMNS     col " + 
		  " where drh.DP_RULES_HEADER_ID = drl.DP_RULES_HEADER_ID " + 
		  "   and drl.SOURCE_TAB_COLUMN_NAME = col.COLUMN_NAME " + 
		  "   and drh.ENABLED_FLAG = 'Y' " + 
		  "   and ((? = 'Y' and drh.VALIDATED_FLAG = 'Y') or ? = 'N') " + 
		  "   and now() between ifnull(drh.START_DATE, now()-1) and ifnull(drh.END_DATE, now()+1) " + 
		  "   and drh.SOURCE_TABLE_OWNER = col.TABLE_SCHEMA " + 
		  "   and drh.SOURCE_TABLE_NAME = col.TABLE_NAME " + 
		  "   and drh.SOURCE_SYS_KEY = ? " + 
		  "   and col.TABLE_SCHEMA = ? " + 
		  "   and col.TABLE_NAME = ? " + 
		  " order by col.ORDINAL_POSITION, drl.LINE_NUM asc ";
	
	// 维护了规则: 排除规则行表列的其他列
	public static final String QUERY_SOUR_TAB_COLUMNS_NOTIN_RULES
		= "select drh.SOURCE_SYS_KEY, " + 
		  "       drh.SOURCE_TABLE_OWNER, " + 
		  "       drh.SOURCE_TABLE_NAME, " + 
		  "       drh.DEST_TABLE_OWNER, " + 
		  "       drh.DEST_TABLE_NAME, " + 
		  "       ifnull(drh.FROM_CONDITION_TAB_LIST, CONCAT(drh.SOURCE_TABLE_OWNER, '.', drh.SOURCE_TABLE_NAME)) FROM_CONDITION_TAB_LIST, " + 
		  "       drh.WHERE_CONDITION_SQL, " + 
		  "       col.COLUMN_NAME SOURCE_TAB_COLUMN_NAME, " + 
		  "       col.COLUMN_NAME DP_RETURN_COLUMN_VAL, " + 
		  "       col.COLUMN_NAME DEST_TAB_COLUMN_NAME, " + 
		  "       col.COLUMN_NAME " + 
		  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all drh, " + 
		  "       information_schema.COLUMNS     col " + 
		  " where drh.ENABLED_FLAG = 'Y' " + 
		  "   and ((? = 'Y' and drh.VALIDATED_FLAG = 'Y') or ? = 'N') " + 
		  "   and now() between ifnull(drh.START_DATE, now()-1) and ifnull(drh.END_DATE, now()+1) " + 
		  "   and drh.SOURCE_TABLE_OWNER = col.TABLE_SCHEMA " + 
		  "   and drh.SOURCE_TABLE_NAME = col.TABLE_NAME " + 
		  "   and drh.SOURCE_SYS_KEY = ? " + 
		  "   and col.TABLE_SCHEMA = ? " + 
		  "   and col.TABLE_NAME = ? " + 
		  "   and not exists ( select 1 " + 
          "                      from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_lines_all drl " + 
          "                     where drh.DP_RULES_HEADER_ID = drl.DP_RULES_HEADER_ID " + 
          "                       and drl.SOURCE_TAB_COLUMN_NAME = col.COLUMN_NAME " + 
          "                   ) " + 
		  " order by col.ORDINAL_POSITION asc ";
	
	// 未维护规则: 取出表对应的列
	public static final String QUERY_TAB_COLUMNS
 	= "select col.COLUMN_NAME " +
 	  "  from information_schema.COLUMNS col " + 
      " where col.TABLE_SCHEMA = ? " + 
 	  "   and col.TABLE_NAME = ? " + 
      " order by col.ORDINAL_POSITION asc";
	
	// 检查快码是否维护
	public static final String CHK_DCETL_SOUR_SYS_KEY_LOOKUP
		= "SELECT count(1) AS TOTAL_ROW_COUNT " + 
          " FROM " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    lkv_sys, " + 
          "      " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    lkv, " + 
          "      " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values_tl lkv_tl " + 
          "WHERE lkv_sys.ENABLE_FLAG = 'Y' " + 
          "  AND now() BETWEEN ifnull(lkv_sys.START_DATE_ACTIVE, now() - 1) AND " + 
          "      ifnull(lkv_sys.END_DATE_ACTIVE, now() + 1) " + 
          "  AND lkv_sys.LOOKUP_CODE = ifnull(?, lkv_sys.LOOKUP_CODE) " + 
          "  AND lkv_sys.LOOKUP_TYPE = lkv.LOOKUP_CODE " + 
          "  AND lkv.LOOKUP_TYPE = lkv_tl.LOOKUP_TYPE " + 
          "  AND lkv.LOOKUP_CODE = lkv_tl.LOOKUP_CODE " + 
          "  AND lkv_tl.NAME = ? " + 
          "  AND lkv.ENABLE_FLAG = 'Y' " + 
          "  AND now() BETWEEN ifnull(lkv.START_DATE_ACTIVE, now() - 1) AND " + 
          "      ifnull(lkv.END_DATE_ACTIVE, now() + 1) " + 
          "  AND lkv.LOOKUP_TYPE = ? " + 
          " LIMIT 1 ";
	
	// 查询data purge 表清单，需要根据快码进行过滤
	public static final String QUERY_DP_INSTANCE_CONFIGS
		= "select cfg.DP_INST_CONFIG_ID, " + 
	      "       cfg.SOURCE_DP_TABLE_OWNER, " + 
          "       cfg.SOURCE_DP_TABLE_NAME, " + 
		  "       cfg.SOURCE_DP_TAB_FILTER_CONDIT1, " + 
		  "       cfg.SOURCE_DP_TAB_FILTER_CONDIT2, " + 
          "	      cfg.SOURCE_DP_TAB_FILTER_CONDIT3, " + 
		  "	      cfg.SOURCE_DP_TAB_FILTER_CONDIT4, " + 
          "       cfg.SOURCE_DP_TAB_FILTER_CONDIT5, " + 
          "       cfg.DEST_DP_TABLE_OWNER, " + 
          "       cfg.DEST_DP_TABLE_NAME, " + 
          "       ifnull(autc.DO_DP_PER_SECONDS, cfg.DO_DP_PER_SECONDS) DO_DP_PER_SECONDS, " + 
          "	      ifnull(autc.DP_TOLERANCE_SECONDS, cfg.DP_TOLERANCE_SECONDS) DP_TOLERANCE_SECONDS, " + 
          "       autc.LAST_DP_DATETIME, " + 
          "       autc.DP_INST_CONFIG_AUTOC_ID, " +
          "       ifnull(autc.SOURCE_SYS_KEY, ?) SOURCE_SYS_KEY" + 
          "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_inst_configs cfg " + 
          "  left join " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_inst_configs_autoc autc " + 
          "       ON (autc.DP_INST_CONFIG_ID = cfg.DP_INST_CONFIG_ID and autc.source_sys_key = ? ) " + 
          " where 1 = 1 " + 
          "   and (autc.enabled_flag = 'Y' OR autc.enabled_flag IS NULL) " + 
          "   /*and ( date_add(cfg.LAST_DP_DATETIME, interval cfg.DO_DP_PER_SECONDS second) <= now() or cfg.LAST_DP_DATETIME is null ) */" + 
          "   and ( date_add(autc.LAST_DP_DATETIME, interval autc.DO_DP_PER_SECONDS second) <= now() or autc.LAST_DP_DATETIME is null )" + 
          "   and cfg.ENABLED_FLAG = 'Y' " + 
          "   and cfg.SOURCE_DP_TABLE_OWNER = ifnull(?, cfg.SOURCE_DP_TABLE_OWNER) " + 
          "   and cfg.SOURCE_DP_TABLE_NAME = ifnull(?, cfg.SOURCE_DP_TABLE_NAME) " + 
          "   and exists ( select 1 " + 
		  "	                 from " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    lkv_sys, " + 
		  "	                      " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    lkv, " + 
		  "	                      " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values_tl lkv_tl " + 
		  "	                where lkv_sys.LOOKUP_CODE = cfg.SOURCE_DP_TABLE_NAME " + 
		  "	                  and lkv_sys.ENABLE_FLAG = 'Y' " + 
		  "	                  and now() between ifnull(lkv_sys.START_DATE_ACTIVE, now()-1) and ifnull(lkv_sys.END_DATE_ACTIVE, now()+1) " + 
		  "	                  and lkv_sys.LOOKUP_TYPE = lkv.LOOKUP_CODE " + 
		  "	                  and lkv.LOOKUP_TYPE = lkv_tl.LOOKUP_TYPE " + 
		  "	                  and lkv.LOOKUP_CODE = lkv_tl.LOOKUP_CODE " + 
		  "	                  and lkv_tl.NAME = ?  " + 
		  "	                  and lkv.ENABLE_FLAG = 'Y' " + 
		  "	                  and now() between ifnull(lkv.START_DATE_ACTIVE, now()-1) and ifnull(lkv.END_DATE_ACTIVE, now()+1) " + 
		  "	                  and lkv.LOOKUP_TYPE = ? " +  
		  "	              )";
	
	// 检查当前清洗的表是否存在启用的规则 但 未验证通过
	public static final String CHK_IS_VALID_DP_RULES_SQL
	    = "select ifnull(h.VALIDATED_FLAG, 'N') VALIDATED_FLAG " + 
          "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all h " + 
          " where h.SOURCE_SYS_KEY = ? " + 
          "   and h.SOURCE_TABLE_OWNER = ? " + 
          "   and h.SOURCE_TABLE_NAME = ? " + 
          "   and h.DEST_TABLE_OWNER = ? " + 
          "   and h.DEST_TABLE_NAME = ? " + 
          "   and h.ENABLED_FLAG = 'Y' " + 
          "   and now() between ifnull(h.START_DATE, now()-1) and ifnull(h.END_DATE, now()+1)";
	
	// 获取表的主键/唯一索引列
	public static final String QUERY_TAB_UNIQUE_CONSTRAINT_COL 
		= "select kcu.COLUMN_NAME " + 
          "  from information_schema.KEY_COLUMN_USAGE kcu " + 
          " where kcu.TABLE_SCHEMA = ? " + 
          "   and kcu.TABLE_NAME = ? " + 
          "   and kcu.CONSTRAINT_NAME = 'PRIMARY' " + 
          " order by kcu.ORDINAL_POSITION ";
	
	// 更新配置表的上一次数据清洗时间
	public static final String UPDATE_DP_CONFIG_LAST_DP_DATETIME
		= "insert into " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_inst_configs_autoc " + 
		  "( DP_INST_CONFIG_AUTOC_ID, " + 
          "  DP_INST_CONFIG_ID, " + 
          "  SOURCE_SYS_KEY, " + 
          "  SOUR_SYS_DP_TAB_FILTER_CONDIT1, " + 
          "  SOUR_SYS_DP_TAB_FILTER_CONDIT2, " + 
          "  SOUR_SYS_DP_TAB_FILTER_CONDIT3, " + 
          "  SOUR_SYS_DP_TAB_FILTER_CONDIT4, " + 
          "  SOUR_SYS_DP_TAB_FILTER_CONDIT5, " + 
          "  DO_DP_PER_SECONDS, " + 
          "  DP_TOLERANCE_SECONDS, " + 
          "  LAST_DP_DATETIME, " + 
          "  ENABLED_FLAG, " + 
          "  CREATION_DATE, " + 
          "  CREATED_BY, " + 
          "  LAST_UPDATE_DATE, " + 
          "  LAST_UPDATED_BY, " + 
          "  LAST_UPDATE_LOGIN " + 
          ") values (?,?,?,?,?,?,?,?,?,?,case when (? = '" + GlobalInfo.NOW_DATE_VAL_MODE + "') then " + 
		  "                                     now() " + 
		  "                                   else date_format(?,'%Y-%m-%d %H:%i:%s') " +
		  "                              end,'Y',now(),null,now(),null,USER()) " + 
		  " ON DUPLICATE KEY " +   
		  " UPDATE LAST_DP_DATETIME = case when (? = '" + GlobalInfo.NOW_DATE_VAL_MODE + "') then " + 
		  "                                  now() " + 
		  "                                else date_format(?,'%Y-%m-%d %H:%i:%s') " +
		  "                           end, " + 
		  "        LAST_UPDATE_DATE = now(), " + 
		  "        LAST_UPDATE_LOGIN = USER() ";
	
	// 插入data purge instance log 日志
	public static final String INSERT_DP_INSTANCE_LOG
		= "insert into " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_instance_log " + 
          "( DP_INSTANCE_LOG_ID," + 
          "  DP_PROGRAM_NAME," + 
          "  SOURCE_SYS_KEY," + 
          "  SOURCE_DP_TABLE_OWNER," + 
          "  SOURCE_DP_TABLE_NAME," + 
          "  DEST_DP_TABLE_OWNER," + 
          "  DEST_DP_TABLE_NAME," + 
          "  START_DP_TIME," + 
          "  END_DP_TIME," + 
          "  PROCESS_STATUS," + 
          "  PROCESS_MESSAGE," + 
          "  PROCESS_DP_SQL_TEXT," + 
          "  LOG_FILE," + 
          "  UPDATED_LAST_DP_DATETIME," + 
          "  DP_PARAMETER1," + 
          "  DP_PARAMETER2," + 
          "  DP_PARAMETER3," + 
          "  DP_PARAMETER4," + 
          "  DP_PARAMETER5," + 
          "  PROCESS_ROW_COUNT," + 
          "  CREATION_DATE," + 
          "  CREATED_BY," + 
          "  LAST_UPDATE_DATE," + 
          "  LAST_UPDATED_BY," + 
          "  LAST_UPDATE_LOGIN" + 
          ") values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),null,now(),null,USER()) ";
	
	
	// ProcessFlowInit: Get source_sys_key and table list
	// 原来的逻辑，因为改造注释之，采用下面新的逻辑
	public static final String QUERY_SOURCE_SYS_KEY_TAB_LIST 
	  = "select distinct CONCAT(alvt.NAME, '.', cfg.SOURCE_DP_TABLE_OWNER, '.', cfg.SOURCE_DP_TABLE_NAME) AS SOURCE_SYS_KEY " + 
		"  from " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    alv_s, " + 
	    "       " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_inst_configs cfg, " + 
		"       " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    alv, " + 
		"       " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values_tl alvt " + 
		" where alv_s.LOOKUP_TYPE = alv.LOOKUP_CODE " + 
		"   and ifnull(alv_s.START_DATE_ACTIVE, now()) <= now() " + 
		"   and ifnull(alv_s.END_DATE_ACTIVE, now()) >= now() " + 
		"   and alv_s.ENABLE_FLAG = 'Y' " + 
		"   and alv_s.LOOKUP_CODE = cfg.SOURCE_DP_TABLE_NAME " + 
		"   and cfg.ENABLED_FLAG = 'Y' " + 
		"   and alvt.LOOKUP_TYPE = alv.LOOKUP_TYPE " + 
		"   and alvt.LOOKUP_CODE = alv.LOOKUP_CODE " + 
		"   and ifnull(alv.START_DATE_ACTIVE, now()) <= now() " + 
		"   and ifnull(alv.END_DATE_ACTIVE, now()) >= now() " + 
		"   and alv.ENABLE_FLAG = 'Y' " + 
		//" and alvt.NAME = 'highly'  " +
		"   and alv.LOOKUP_TYPE = ? " + 
		" order by 1 ";
	
	// ProcessFlowInit: Get source_sys_key and table list(五表关联)
	public static final String QUERY_SOURCE_SYS_KEY_TAB_LIST_NEW
	  = "select " + 
	    //"       distinct CONCAT(dptq.source_sys_key, '.', cfg.SOURCE_DP_TABLE_OWNER, '.', cfg.SOURCE_DP_TABLE_NAME) AS SOURCE_SYS_KEY " + 
	    "       distinct dptq.source_sys_key, dptq.process_group_id, dptqd.type, " + 
	    "                cfg.SOURCE_DP_TABLE_OWNER AS value1, " +
	    "                cfg.SOURCE_DP_TABLE_NAME AS value2, " + 
	    "                dptqd.value3, dptqd.value4, dptqd.value5 " + 
		"  from " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    alv_s, " + 
	    "       " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_inst_configs cfg, " + 
		"       " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values    alv, " + 
		"       " + GlobalInfo.DB_RMS_OWNER + ".afwk_lookup_values_tl alvt, " + 
		"		" + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues dptq, " +
        "   	" + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queue_details dptqd " +
		" where alv_s.LOOKUP_TYPE = alv.LOOKUP_CODE " + 
		"   and ifnull(alv_s.START_DATE_ACTIVE, now()) <= now() " + 
		"   and ifnull(alv_s.END_DATE_ACTIVE, now()) >= now() " + 
		"   and alv_s.ENABLE_FLAG = 'Y' " + 
		"   and alv_s.LOOKUP_CODE = cfg.SOURCE_DP_TABLE_NAME " + 
		"   and cfg.ENABLED_FLAG = 'Y' " + 
		"   and alvt.LOOKUP_TYPE = alv.LOOKUP_TYPE " + 
		"   and alvt.LOOKUP_CODE = alv.LOOKUP_CODE " + 
		"   and ifnull(alv.START_DATE_ACTIVE, now()) <= now() " + 
		"   and ifnull(alv.END_DATE_ACTIVE, now()) >= now() " + 
		"   and alv.ENABLE_FLAG = 'Y' " + 
	    "   and upper(cfg.SOURCE_DP_TABLE_OWNER) = upper(dptqd.value1) " +
	    "   and upper(cfg.SOURCE_DP_TABLE_NAME) = (dptqd.value2) " +
		"   and upper(alvt.NAME) = upper(dptq.source_sys_key) " +
	    "   and dptq.process_group_id = dptqd.process_group_id " +
	    "   and alv.LOOKUP_TYPE = ? " + 
	    "   and dptq.task_queue_id = ifnull(?, dptq.task_queue_id) " + 
	    "   and dptq.source_sys_key = ? " +
	    "   and dptq.process_group_id = ? " +
	    "   and dptq.source_ref_doc_id = ? " +
	    "	and dptq.status_code = ? " +
	    "	and dptq.task_type_code = ? " + //DCETL
		"	and dptqd.type = ? " + //DCETL_TAB_NAME
	    "   and dptq.enabled_flag = 'Y' " +
	    "	and dptqd.process_flag = 'N' " +
		" order by 1 ";
	
	// Get concurrent control flag
	public static final String QUERY_CONCURRENT_CTRL_INFO
	  = "select dpcc.DP_CONCURRENT_CONTROL_ID, " + 
	    "       dpcc.CONCURRENT_STATUS, " + 
        "       TIMESTAMPDIFF(SECOND, dpcc.LAST_UPDATE_DATE, DATE_FORMAT(NOW(),'%Y-%m-%d %H:%i:%s')) DIFF_SECONDS, " + 
        "       dpcc.TIMEOUT_SECONDS " + 
        "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_concurrent_control dpcc " + 
        " where dpcc.SOURCE_SYS_KEY =  ? " + 
        "   and dpcc.SOURCE_TABLE_OWNER =  ? " + 
        "   and dpcc.SOURCE_TABLE_NAME =  ? " + 
	    "   and dpcc.DEST_TABLE_OWNER =  ? " + 
	    "   and dpcc.DEST_TABLE_NAME = ? ";
	
	// 插入记录到并发控制的表
	public static final String INSERT_DP_CONCURRENT_CTRL
	  = "insert into " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_concurrent_control " + 
        "( DP_CONCURRENT_CONTROL_ID, " + 
        "  SOURCE_SYS_KEY, " + 
        "  SOURCE_TABLE_OWNER, " + 
        "  SOURCE_TABLE_NAME, " + 
        "  DEST_TABLE_OWNER, " + 
        "  DEST_TABLE_NAME, " + 
        "  CONCURRENT_STATUS, " + 
        "  TIMEOUT_SECONDS, " + 
        "  CREATION_DATE, " + 
        "  CREATED_BY, " + 
        "  LAST_UPDATE_DATE, " + 
        "  LAST_UPDATED_BY, " + 
        "  LAST_UPDATE_LOGIN " + 
        ") values (?,?,?,?,?,?,?,?,now(),null,now(),null,USER())";
	
	// 更新并发控制表的记录
	public static final String UPDATE_DP_CONCURRENT_CTRL
	  = "update " + GlobalInfo.DEST_DB_OWNER + ".dp_data_purge_concurrent_control " + 
        "   set CONCURRENT_STATUS = ? , " + 
        "       LAST_UPDATE_DATE = now() " + 
        " where DP_CONCURRENT_CONTROL_ID = ? ";
	
	//查找一条DCETL的 PENDING 状态满足条件的数据（已获取锁的方式）
	//如果有多条满足条件，则只取一条
	public static final String QUERY_PROCESS_TASK_QUEUE_LIMIT1
	   = "select task_queue_id, task_type_code, source_sys_key, process_group_id, source_ref_doc_id, status_code " + 
		 "  from " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues " + 
		 " where status_code = '" + GlobalInfo.DC_QUEUES_PENDING_STS + "' " + 
		 "   and task_type_code = '" + GlobalInfo.DC_QUEUES_TASK_TYPE + "' " +
		 "   and enabled_flag = 'Y' " + 
		 "   and source_sys_key = ifnull(?, source_sys_key) " + 
		 "   and process_group_id = ifnull(?, process_group_id) " + 
		 "   and source_ref_doc_id = ifnull(?, source_ref_doc_id) " + 
		 " limit 1 " + 
		 " for update ";
	
	//根据mq获取dp_table_owner + dp_table_name
	public static final String QUERY_LISTENER_SOURCE_SYS_KEY_TAB 
			= "select distinct CONCAT(dptq.source_sys_key, '.', dptqd.value1, '.' ,dptqd.value2) AS DCETL_TAB_INFO " + 
			  "	 from " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queue_details dptqd, " + 
			  "		  " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues dptq " +
			  " where dptqd.process_group_id = dptq.process_group_id " +
			  "   and dptq.enabled_flag = 'Y' " + 
			  "   and dptq.source_sys_key = ? " + 
			  "	  and dptq.task_type_code = ? " + //DCETL
			  "   and dptq.status_code = ? " + //PENDING
			  "	  and dptqd.type = ? " + //DCETL_TAB_NAME
			  "   and dptqd.process_flag = 'N' " + 
			  "   and dptq.process_group_id = ? ";
		
	//根据mq修改数据清洗的状态(PENDING => RUNNING => SUCCESS)／日期／处理信息等字段
	public static final String UPDATE_DP_TASK_QUEUES_STATUS
			= "update " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues dptq " + 
			  "	  set dptq.status_code = ?, " + 
			  "       dptq.process_message = ?, " + 
			  "       dptq.start_process_datetime = CASE WHEN (? = 'Y') THEN now() ELSE dptq.start_process_datetime END, " + 
			  "       dptq.end_process_datetime = CASE WHEN (? = 'Y') THEN now() ELSE dptq.end_process_datetime END, " + 
			  "	      dptq.last_update_date = now(), " +
			  "	      dptq.last_updated_by = user() " + 
			  " where dptq.enabled_flag = 'Y' " + 
			  "   and dptq.task_queue_id = ifnull(?, dptq.task_queue_id) " + 
			  "   and dptq.task_type_code = ? " + 
			  "   and dptq.source_sys_key = ? " + 
			  "   and dptq.process_group_id = ? ";
		
		//修改details数据的状态 N => Y
		public static final String UPDATE_DC_DETAILS_PROCESS_FLAG
			= "update " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queue_details dptqd" + 
			  "	  set dptqd.process_flag = ? , " + 
			  "       dptqd.process_message = ? , " + 
			  "		  dptqd.last_update_date = now() " +
			  " where dptqd.type = ? " + 
			  "	  and dptqd.process_group_id = ? " +
			  "   and dptqd.value1 = ? " +
			  "   and dptqd.value2 = ? ";
		
		//迁移数据到queues => queues_log
		public static final String MOVE_QUEUES_2_QUEUES_LOGS 
			= "insert into " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues_logs " + 
			  "  (task_queue_id,task_type_code,source_sys_key,process_group_id,status_code,process_message,start_process_datetime," + 
			  "   end_process_datetime,description,enabled_flag,creation_date,created_by,last_update_date,last_updated_by,version,source_ref_doc_id) " + 
			  "select task_queue_id,task_type_code,source_sys_key,process_group_id,status_code,process_message,start_process_datetime," + 
			  "       end_process_datetime,description,enabled_flag,creation_date,created_by,last_update_date,last_updated_by,version,source_ref_doc_id " + 
			  "  from " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues " + 
			  " where enabled_flag = 'Y' " + 
			  "   and task_type_code = ? " + //DCETL
			  "   and status_code = ? " +  //SUCCESS
			  "   and process_group_id = ? " + 
			  "   and source_sys_key = ? ";
		
		//迁移数据details => details_log
		public static final String MOVE_DETAILS_2_DETAILS_LOGS 
			= "insert into " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queue_details_logs" +
		      "   (process_group_id,type,value1,value2,value3,value4,value5,process_flag,creation_date,last_update_date,process_message) " + 
			  "select process_group_id,type,value1,value2,value3,value4,value5,process_flag," + 
		      "       creation_date,last_update_date,process_message " + 
			  "  from " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queue_details " + 
			  " where type = ? " +
			  "   and process_group_id = ? ";
		
		
		//删除queues表中已处理的数据
		public static final String DELETE_TASK_QUEUES_INFO
			= "delete from " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queues " +
			  "      where enabled_flag = 'Y' " + 
			  "        and task_type_code = ? " + 
			  "        and status_code = ? " + 
			  "        and process_group_id = ? " + 
			  "        and source_sys_key = ? ";
		
		//删除details表中已处理的数据
		public static final String DELETE_TASK_QUEUES_DETAILS_INFO
			= "delete from " + GlobalInfo.DB_MONITOR_OWNER + ".dc_process_task_queue_details " + 
			  "		 where type = ? " + 
			  "        and process_group_id = ? ";
              
}
