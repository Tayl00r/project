/**
 * 
 */
package utility;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import config.GlobalInfo;
import config.GlobalSql;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;
import entity.DcSysDbConnMapping;
import entity.QueryDPInstanceConfigsRsEO;
import entity.QuerySourceTabColRsEO;
import entity.QueryTabColumnsRsEO;

/**
 * @author Administrator
 *
 */
public class DataPurgeETL {
	
	private DBHelper dcETLHelper = null;
	private String logExecuteSqlSummary = null;

	public DBHelper getDcETLHelper() {
		return dcETLHelper;
	}

	public void setDcETLHelper(DBHelper dcETLHelper) {
		this.dcETLHelper = dcETLHelper;
	}

	// 获取查询源表数据的sql，根据参数进行拼接
	// Select [sourceTabColumnsList] from [sourceTabName] where [sourceTabWhereClause]
	private String generateSourceQuerySql(IfaceParameters ifaceParameter,
			                              String sourceTabOwner,
			                              String sourceTabName,
			                              String sourceTabColumnsList,
			                              String sourceTabWhereClause,
			                              String leftJoinClause) {
		// 参数sourceTabName已经包括了schema，或者组合的表，所以不需要添加sourceTabOwner了
		String selectSql = null;
		String tabAliasName = null;
		if (sourceTabName.contains(",")) {
			tabAliasName = "";
		} else {
			tabAliasName = GlobalInfo.SOUR_TAB_ALIAS_NAME;
		}
		if (sourceTabWhereClause != null && !"".equals(sourceTabWhereClause.trim())) {
			selectSql = "select " + sourceTabColumnsList + " from " + 
					(sourceTabName.contains(".")?sourceTabName:sourceTabOwner+"."+sourceTabName) + 
					" " + tabAliasName + GlobalInfo.LINE_SEPARATOR + 
					leftJoinClause + GlobalInfo.LINE_SEPARATOR + 
					" where (" + sourceTabWhereClause + ")";
		} else {
			selectSql = "select " + sourceTabColumnsList + " from " + 
					(sourceTabName.contains(".")?sourceTabName:sourceTabOwner+"."+sourceTabName) + 
					" " + tabAliasName + GlobalInfo.LINE_SEPARATOR + 
					leftJoinClause + GlobalInfo.LINE_SEPARATOR + 
					" where (" + sourceTabWhereClause + ")";  // (1=1)
		}
		return selectSql;
		
	}
	
	// Insert into [Owner].[TableName](...)
	private String generateDestInsertSql(IfaceParameters ifaceParameter,
										 String destTabOwner,
										 String destTabName,
										 String destTabColumnsList) {
		String insertSql = "insert into " + destTabOwner + "." + destTabName + 
				"(" + destTabColumnsList + ")";
		return insertSql;
		
	}
	
	// 获取data purge 表清单
	public List<QueryDPInstanceConfigsRsEO> getDataPurgeTabLists(String sourceSysKey,
			                                                     String sourceTabOwner,
			                                                     String sourceTabName) 
	        throws ClassNotFoundException, SQLException, ParseException {
		
		LogManager.appendToLog("Entering getDataPurgeTabLists() ---> ");
		
		List<QueryDPInstanceConfigsRsEO> dpTabList = 
				new ArrayList<QueryDPInstanceConfigsRsEO>();
		
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		                				    GlobalInfo.DB_DCETL_URL, 
		                				    GlobalInfo.DB_DCETL_USER_NAME,
		                				    GlobalInfo.DB_DCETL_PASSWORD, 
		                				    null);
		}
		String[] args = new String[]{ sourceSysKey, sourceSysKey, sourceSysKey, 
				                      sourceTabOwner, sourceTabName, sourceSysKey, 
		          					  GlobalInfo.LOOKUP_TYPE_DCETL_SOUR_SYS_KEY };
		
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.QUERY_DP_INSTANCE_CONFIGS, (Object[])args);
			while (dcETLHelper.resultSet.next()) {
				QueryDPInstanceConfigsRsEO rsEO = new QueryDPInstanceConfigsRsEO();
				
				rsEO.setDp_inst_config_id(dcETLHelper.resultSet.getString("DP_INST_CONFIG_ID"));
				rsEO.setSource_dp_table_owner(dcETLHelper.resultSet.getString("SOURCE_DP_TABLE_OWNER"));
				rsEO.setSource_dp_table_name(dcETLHelper.resultSet.getString("SOURCE_DP_TABLE_NAME"));
				rsEO.setSource_dp_tab_filter_condit1(dcETLHelper.resultSet.getString("SOURCE_DP_TAB_FILTER_CONDIT1"));
				rsEO.setSource_dp_tab_filter_condit2(dcETLHelper.resultSet.getString("SOURCE_DP_TAB_FILTER_CONDIT2"));
				rsEO.setSource_dp_tab_filter_condit3(dcETLHelper.resultSet.getString("SOURCE_DP_TAB_FILTER_CONDIT3"));
				rsEO.setSource_dp_tab_filter_condit4(dcETLHelper.resultSet.getString("SOURCE_DP_TAB_FILTER_CONDIT4"));
				rsEO.setSource_dp_tab_filter_condit5(dcETLHelper.resultSet.getString("SOURCE_DP_TAB_FILTER_CONDIT5"));
				// 分库分表的处理
				//if (GlobalInfo.sysDbConnMapping.getDb_schema() != null && !"".equals(GlobalInfo.sysDbConnMapping.getDb_schema())) {
				//	rsEO.setDest_dp_table_owner(GlobalInfo.sysDbConnMapping.getDb_schema());
				//} else {
					rsEO.setDest_dp_table_owner(dcETLHelper.resultSet.getString("DEST_DP_TABLE_OWNER"));
				//}
				rsEO.setDest_dp_table_name(dcETLHelper.resultSet.getString("DEST_DP_TABLE_NAME"));
				rsEO.setDo_dp_per_seconds(dcETLHelper.resultSet.getInt("DO_DP_PER_SECONDS"));
				rsEO.setDp_tolerance_seconds(dcETLHelper.resultSet.getInt("DP_TOLERANCE_SECONDS"));
				if (dcETLHelper.resultSet.getDate("LAST_DP_DATETIME") != null) {
					rsEO.setLast_dp_datetime(TypeConversionUtil.dateCheck(dcETLHelper.resultSet.getDate("LAST_DP_DATETIME").toString() + " " + 
							dcETLHelper.resultSet.getTime("LAST_DP_DATETIME").toString()));
				} else {
					rsEO.setLast_dp_datetime(dcETLHelper.resultSet.getDate("LAST_DP_DATETIME"));
				}
				rsEO.setDp_inst_config_autoc_id(dcETLHelper.resultSet.getString("DP_INST_CONFIG_AUTOC_ID"));
				rsEO.setSource_sys_key(dcETLHelper.resultSet.getString("SOURCE_SYS_KEY"));
				rsEO.setDp_rules_header_id(dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID"));  // 同时取出规则头
				
				LogManager.appendToLog("QueryDPInstanceConfigsRsEO-rsEO:" + rsEO.toString(), GlobalInfo.DEBUG_MODE);
				
				dpTabList.add(rsEO);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("getDataPurgeTabLists() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
		
		LogManager.appendToLog("Leaving getDataPurgeTabLists() ---> ");
		
		return dpTabList;
	}
	
	// 检查当前清洗的表是否存在启用的规则 但 未验证通过
	public boolean checkIsValidDpRules(String sourceSysKey,
            						   String sourceDpTableOwner,
            						   String sourceDpTableName,
            						   String destDpTableOwner,
            						   String destDpTableName) 
            throws ClassNotFoundException, SQLException {
	    
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
						    				GlobalInfo.DB_DCETL_URL, 
						    				GlobalInfo.DB_DCETL_USER_NAME,
						    				GlobalInfo.DB_DCETL_PASSWORD, 
						    				null);
		}
		String[] args = new String[]{ sourceSysKey, sourceDpTableOwner, sourceDpTableName,
									  destDpTableOwner, destDpTableName};
		String validatedFlag = "N";
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.CHK_IS_VALID_DP_RULES_SQL, (Object[])args);
			if (dcETLHelper.resultSet.next()) {
				validatedFlag = dcETLHelper.resultSet.getString("VALIDATED_FLAG");
			} else {
				validatedFlag = "Y";  // 不存在有效的规则，不对validated_flag做判断
			}
				
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("checkIsValidDpRules() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
			
		LogManager.appendToLog("validatedFlag:" + validatedFlag, GlobalInfo.STATEMENT_MODE);
			
		if ("Y".equals(validatedFlag)) 
			return true;
		else
			return false;
	}
	
	// 根据表参数，获取data purge 列信息（维护了规则的表列）
	public List<QuerySourceTabColRsEO> getDataPurgeTabColumnLists(String sourceSysKey,
            													  String sourceTabOwner,
            													  String sourceTabName,
            													  String validateFlag,
			                                                      boolean inRulesFlag) 
			throws ClassNotFoundException, SQLException {
		
		List<QuerySourceTabColRsEO> dpTabColumnList = 
				new ArrayList<QuerySourceTabColRsEO>();
		
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		                				    GlobalInfo.DB_DCETL_URL, 
		                				    GlobalInfo.DB_DCETL_USER_NAME,
		                				    GlobalInfo.DB_DCETL_PASSWORD, 
		                				    null);
		}
		String[] args = new String[]{ validateFlag, validateFlag, sourceSysKey, sourceTabOwner, sourceTabName };
		try {
			// execute query
			if (inRulesFlag) {
				// 获取在清洗规则中存在维护的列信息
				dcETLHelper.executeQuery(GlobalSql.QUERY_SOUR_TAB_COLUMNS_IN_RULES, (Object[])args);
			} else {
				// 获取在清洗规则中不存在维护的列信息
				dcETLHelper.executeQuery(GlobalSql.QUERY_SOUR_TAB_COLUMNS_NOTIN_RULES, (Object[])args);
			}
			while (dcETLHelper.resultSet.next()) {
				QuerySourceTabColRsEO rsEO = new QuerySourceTabColRsEO();
				
				rsEO.setDp_rules_header_id(dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID"));  // Added by chao.tang@2017-02-10 for 日志分析
				rsEO.setSource_sys_key(dcETLHelper.resultSet.getString("SOURCE_SYS_KEY"));
				rsEO.setSource_table_owner(dcETLHelper.resultSet.getString("SOURCE_TABLE_OWNER"));
				rsEO.setSource_table_name(dcETLHelper.resultSet.getString("SOURCE_TABLE_NAME"));
				rsEO.setDest_table_owner(dcETLHelper.resultSet.getString("DEST_TABLE_OWNER"));
				rsEO.setDest_table_name(dcETLHelper.resultSet.getString("DEST_TABLE_NAME"));
				rsEO.setFrom_condition_tab_list(dcETLHelper.resultSet.getString("FROM_CONDITION_TAB_LIST"));
				rsEO.setWhere_condition_sql(dcETLHelper.resultSet.getString("WHERE_CONDITION_SQL"));
				rsEO.setSource_tab_column_name(dcETLHelper.resultSet.getString("SOURCE_TAB_COLUMN_NAME"));
				rsEO.setDp_return_column_val(dcETLHelper.resultSet.getString("DP_RETURN_COLUMN_VAL"));
				rsEO.setDest_tab_column_name(dcETLHelper.resultSet.getString("DEST_TAB_COLUMN_NAME"));
				rsEO.setColumn_name(dcETLHelper.resultSet.getString("COLUMN_NAME"));
				
				dpTabColumnList.add(rsEO);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("getDataPurgeTabColumnLists() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
		
		return dpTabColumnList;
	}
	
	// 获取维护的规则头ID
	public String getDpRulesHeaderId(String sourceSysKey,
									 String sourceTabOwner,
									 String sourceTabName) throws ClassNotFoundException, SQLException {
						
		String dpRulesHeaderId = null;
						
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
								GlobalInfo.DB_DCETL_URL, 
								GlobalInfo.DB_DCETL_USER_NAME,
								GlobalInfo.DB_DCETL_PASSWORD, 
								null);
		}
		String[] args = new String[]{ sourceTabOwner, sourceTabName, sourceSysKey };
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.QUERY_SOUR_TAB_RULES_HEADER_ID, (Object[])args);

			if (dcETLHelper.resultSet.next()) {
				dpRulesHeaderId = dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("getDpRulesHeaderId() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
						
		return dpRulesHeaderId;
	}

	// 输出调试信息，为了方便调试和定位取不到规则的原因
	public void printDebugStackInfo(String sourceSysKey,
									String sourceTabOwner,
									String sourceTabName,
									String destTabOwner,
									String destTabName,
									String dpRulesHeaderId) {
		String debugMsgPrefix = "printDebugStackInfo["+sourceSysKey+"~"+sourceTabName+"]:>>";
		
		LogManager.appendToLog(debugMsgPrefix+"*****************************************************************");
		LogManager.appendToLog(debugMsgPrefix+"****************************** START ****************************");
		LogManager.appendToLog(debugMsgPrefix+"*****************************************************************");
		
		try {
			LogManager.appendToLog(debugMsgPrefix + "[" + sourceSysKey + "][" + sourceTabOwner + "." + sourceTabName + 
					"][" + destTabOwner + "." + destTabName + "][dpRulesHeaderId=" + dpRulesHeaderId + "]");
			
			if (dcETLHelper == null) {
				/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
									GlobalInfo.DB_DCETL_URL, 
									GlobalInfo.DB_DCETL_USER_NAME,
									GlobalInfo.DB_DCETL_PASSWORD, 
									null);
			}
			
			LogManager.appendToLog(debugMsgPrefix + "(1)Get dp_rules_headers_all info");
			// execute query
			String sql = "select drh.DP_RULES_HEADER_ID, drh.ENABLED_FLAG " + 
					  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all drh " +  
					  " where drh.ENABLED_FLAG = 'Y' " + 
					  "   and drh.VALIDATED_FLAG = 'Y' " + 
					  "   and now() between ifnull(drh.START_DATE, now()-1) and ifnull(drh.END_DATE, now()+1) " + 
					  "   and drh.SOURCE_TABLE_OWNER = ? " + 
					  "   and drh.SOURCE_TABLE_NAME = ? " + 
					  "   and drh.SOURCE_SYS_KEY = ? ";
			dcETLHelper.executeQuery(sql, (Object[]) new String[]{ sourceTabOwner, sourceTabName, sourceSysKey });
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[1.1]" + dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID") + "~" + 
						dcETLHelper.resultSet.getString("ENABLED_FLAG") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(2)Get dp_rules_headers_all info by DpRulesHeaderId");
			// execute query
			sql = "select drh.DP_RULES_HEADER_ID, drh.ENABLED_FLAG " + 
				  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all drh " +  
				  " where drh.DP_RULES_HEADER_ID = ? ";
			dcETLHelper.executeQuery(sql, (Object[]) new String[]{ dpRulesHeaderId });
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[2.1]" + dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID") + "~" + 
						dcETLHelper.resultSet.getString("ENABLED_FLAG") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(3)Get COUNT(dp_rules_headers_all) ");
			// execute query
			sql = "select count(1) AS TOTAL_CNT" + 
				  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all drh ";
			dcETLHelper.executeQuery(sql, null);
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[3.1]" + dcETLHelper.resultSet.getInt("TOTAL_CNT") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(4)Get dp_rules_headers_all limit 3");
			// execute query
			sql = "select drh.DP_RULES_HEADER_ID, drh.ENABLED_FLAG" + 
				  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_headers_all drh  limit 3";
			dcETLHelper.executeQuery(sql, null);
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[4.1]" + dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID") + "~" + 
						dcETLHelper.resultSet.getString("ENABLED_FLAG") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(5)Get dp_rules_lines_all info ");
			// execute query
			sql = "select drl.DP_RULES_HEADER_ID, drl.DP_RULES_LINE_ID, drl.SOURCE_TAB_COLUMN_NAME, drl.DEST_TAB_COLUMN_NAME " + 
				  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_lines_all  drl" +  
				  " where drl.DP_RULES_HEADER_ID = ? ";
			dcETLHelper.executeQuery(sql, (Object[])new String[]{ dpRulesHeaderId });
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[5.1]" + dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID") + "~" + 
						dcETLHelper.resultSet.getString("DP_RULES_LINE_ID") + "~" + 
						dcETLHelper.resultSet.getString("SOURCE_TAB_COLUMN_NAME") + "~" + 
						dcETLHelper.resultSet.getString("DEST_TAB_COLUMN_NAME") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(6)Get COUNT(dp_rules_lines_all) ");
			// execute query
			sql = "select count(1) AS TOTAL_CNT" + 
				  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_lines_all drl ";
			dcETLHelper.executeQuery(sql, null);
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[6.1]" + dcETLHelper.resultSet.getInt("TOTAL_CNT") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(7)Get dp_rules_lines_all info ");
			// execute query
			sql = "select drl.DP_RULES_HEADER_ID, drl.DP_RULES_LINE_ID, drl.SOURCE_TAB_COLUMN_NAME, drl.DEST_TAB_COLUMN_NAME " + 
				  "  from " + GlobalInfo.DEST_DB_OWNER + ".dp_rules_lines_all  drl  limit 5";
			dcETLHelper.executeQuery(sql, null);
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[7.1]" + dcETLHelper.resultSet.getString("DP_RULES_HEADER_ID") + "~" + 
						dcETLHelper.resultSet.getString("DP_RULES_LINE_ID") + "~" + 
						dcETLHelper.resultSet.getString("SOURCE_TAB_COLUMN_NAME") + "~" + 
						dcETLHelper.resultSet.getString("DEST_TAB_COLUMN_NAME") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(8) SELECT * FROM INFORMATION_SCHEMA.INNODB_LOCKS; ");
			// execute query
			sql = "SELECT t.lock_id,t.lock_trx_id,t.lock_mode,t.lock_type,t.lock_table,t.lock_index,t.lock_data FROM INFORMATION_SCHEMA.INNODB_LOCKS t";
			dcETLHelper.executeQuery(sql, null);
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[8.1]" + dcETLHelper.resultSet.getString("lock_id") + "~" + 
						dcETLHelper.resultSet.getString("lock_trx_id") + "~" + 
						dcETLHelper.resultSet.getString("lock_mode") + "~" + 
						dcETLHelper.resultSet.getString("lock_type") + "~" +
						dcETLHelper.resultSet.getString("lock_table") + "~" +
						dcETLHelper.resultSet.getString("lock_index") + "~" + 
						dcETLHelper.resultSet.getString("lock_data") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix + "(9) SELECT * FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS; ");
			// execute query
			sql = "SELECT t.requesting_trx_id,t.requested_lock_id,t.blocking_trx_id,t.blocking_lock_id FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS t";
			dcETLHelper.executeQuery(sql, null);
			while (dcETLHelper.resultSet.next()) {
				LogManager.appendToLog(debugMsgPrefix + "[9.1]" + dcETLHelper.resultSet.getString("requesting_trx_id") + "~" + 
						dcETLHelper.resultSet.getString("requested_lock_id") + "~" + 
						dcETLHelper.resultSet.getString("blocking_trx_id") + "~" + 
						dcETLHelper.resultSet.getString("blocking_lock_id") + "~");
			}
			dcETLHelper.close();
			
			LogManager.appendToLog(debugMsgPrefix+"*****************************************************************");
			LogManager.appendToLog(debugMsgPrefix+"****************************** END ******************************");
			LogManager.appendToLog(debugMsgPrefix+"*****************************************************************");
			
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog(debugMsgPrefix + "Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
		} finally {
			try {
				if (dcETLHelper != null) {
					dcETLHelper.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
						
		LogManager.appendToLog("*************************************************************************************");
		LogManager.appendToLog("*************************************************************************************");
		LogManager.appendToLog("*************************************************************************************");
	}
	
	
	// 根据表参数，获取data purge 列信息（未维护规则的表列）
	public List<QueryTabColumnsRsEO> getDataPurgeTabColumnListsOf(String tableOwner,
	            												  String tableName) 
			throws ClassNotFoundException, SQLException {
			
		List<QueryTabColumnsRsEO> dpTabColumnList = 
				new ArrayList<QueryTabColumnsRsEO>();
			
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
			                				GlobalInfo.DB_DCETL_URL, 
			                				GlobalInfo.DB_DCETL_USER_NAME,
			                				GlobalInfo.DB_DCETL_PASSWORD, 
			                				null);
		}
			String[] args = new String[]{ tableOwner, tableName };
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.QUERY_TAB_COLUMNS, (Object[])args);
			while (dcETLHelper.resultSet.next()) {
				QueryTabColumnsRsEO rsEO = new QueryTabColumnsRsEO();
					
				rsEO.setColumn_name(dcETLHelper.resultSet.getString("COLUMN_NAME"));
					
				dpTabColumnList.add(rsEO);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("getDataPurgeTabColumnListsOf() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
			
		return dpTabColumnList;
	}
	
	// 获取指定表的主键/唯一约束列
	public List<Object> getTabUniqueConstraintCol(String tableOwner, String tableName) 
			throws ClassNotFoundException, SQLException {
		
		List<Object> dpTabColumnList = new ArrayList<Object>();
			
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
			                				GlobalInfo.DB_DCETL_URL, 
			                				GlobalInfo.DB_DCETL_USER_NAME,
			                				GlobalInfo.DB_DCETL_PASSWORD, 
			                				null);
		}
		String[] args = new String[]{ tableOwner, tableName };
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.QUERY_TAB_UNIQUE_CONSTRAINT_COL, (Object[])args);
			while (dcETLHelper.resultSet.next()) {	
				dpTabColumnList.add(dcETLHelper.resultSet.getString("COLUMN_NAME"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("getTabUniqueConstraintCol() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
			
		return dpTabColumnList;
	}
	
	// 方法1: 判断表列是否存在，存在返回true，否则返回false（重载）
	public boolean isTabColumnExists(String tableOwner,
			                         String tableName,
			                         String tableColumnName)
			throws ClassNotFoundException, SQLException {
		
		int totalTabColCount = 0;  // 返回列数合计
		
		if (dcETLHelper == null) {
			/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
					    					GlobalInfo.DB_DCETL_URL, 
					    					GlobalInfo.DB_DCETL_USER_NAME,
					    					GlobalInfo.DB_DCETL_PASSWORD, 
					    					null);
		}
		String[] args = new String[]{ tableOwner, tableName, tableColumnName };
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.CHK_TAB_COLUMN_EXISTS_SQL, (Object[])args);
			if (dcETLHelper.resultSet.next()) {
				totalTabColCount =  dcETLHelper.resultSet.getInt("TOTAL_COL_ROW_COUNT");
			}
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("isTabColumnExists() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
		
		LogManager.appendToLog("totalTabColCount:" + totalTabColCount, GlobalInfo.STATEMENT_MODE);
		
		if (totalTabColCount > 0) 
			return true;
		else
			return false;
	}
	
	// 方法2: 判断表列是否存在，存在返回true，否则返回false（重载）
	public boolean isTabColumnExists(List<QueryTabColumnsRsEO> tableColList,
				                     String tableColumnName)
			throws ClassNotFoundException, SQLException {
			
		int totalTabColCount = 0;  // 返回列数合计
		
		for (int i=0; i<tableColList.size(); i++) {
			if (tableColumnName.equalsIgnoreCase(tableColList.get(i).getColumn_name())) {
				totalTabColCount = totalTabColCount + 1;
				break;
			}
		}
		
		LogManager.appendToLog("totalTabColCount:" + totalTabColCount, GlobalInfo.STATEMENT_MODE);
		
		if (totalTabColCount > 0) 
			return true;
		else
			return false;
	}
	
	// 根据配置的源表列表获取表别名
	private String getFromConditionSourTabAlias(String sourFromTabName, String sourceDpTableName) {
		String tabAlias = GlobalInfo.SOUR_TAB_ALIAS_NAME;  // 默认 sour_t
		String fromTabList = sourFromTabName;
		
		if (fromTabList != null && !"".equals(fromTabList) && fromTabList.contains(",")) {
			// 包含逗号才进行判断
			fromTabList = fromTabList.replaceAll("\\(|\\)|,", " ") + " ";  // 全部替换成空格
			int startPos = fromTabList.indexOf(sourceDpTableName + " ");  // 表名+空格
			fromTabList = fromTabList.substring(startPos);
			fromTabList = fromTabList.replace(sourceDpTableName + " ", "");
			int endPos = fromTabList.indexOf(" ");
			
			if (startPos >= 0 ) {
				if (endPos >= 0) {
					tabAlias = fromTabList.substring(0, endPos);
				} else {
					tabAlias = fromTabList.substring(0);
				}
				//
			}
			LogManager.appendToLog("tabAlias:" + tabAlias, GlobalInfo.STATEMENT_MODE);
		}
		
		return tabAlias;
	}
	
	// 附加源表别名
	private String appendSourTabAlias(List<QueryTabColumnsRsEO> tableColList,
                                      String tableColumnName,
                                      String sourceDpTableName,
                                      String fromConditionSourTabAlias) {
		String returnColumnName = tableColumnName;
		String columnName = null;
		
		for (int i=0; i<tableColList.size(); i++) {
			columnName = tableColList.get(i).getColumn_name();
			
			// 如果已包含"." + columnName，则不进行替换，表名该列已经存在别名，替换容易出错
			if (returnColumnName.contains(sourceDpTableName + "." + columnName)) {
			  returnColumnName = returnColumnName.replaceAll(sourceDpTableName + "." + columnName, fromConditionSourTabAlias + "." + columnName);
			}
		}
		
		LogManager.appendToLog("源tableColumnName: " + tableColumnName + 
				" => 目标returnColumnName: " + returnColumnName, GlobalInfo.DEBUG_MODE);
		
		return returnColumnName.trim();
	}
	
	// 插入data purge 日志
	private boolean insertDpInstanceLog(DBHelper dcETLHelper, List<Object> args) 
			throws ClassNotFoundException, SQLException {
		
		/*
		try {
			//dcETLHelper.prepareSql(GlobalSql.INSERT_DP_INSTANCE_LOG);
			//dcETLHelper.setAndExecuteDML(args);
			dcETLHelper.setAndExecuteDML(GlobalSql.INSERT_DP_INSTANCE_LOG, args);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		*/
		// 启动线程异步执行日志的写入
		try {
			new DpInstanceLogger(args).start();
		} catch (Exception e) {
			// 日志插入失败，不做任何处理
			LogManager.appendToLog("insertDpInstanceLog() => Exception: " + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
		}
		
		return true;
	}
	
	// 获取下一次同步时间
	private Date getNewLastDpDatetime(Date rs_last_dp_datetime,
			                          Date currentSysDatetime,
			                          Date paramDpStartDate,
			                          Date paramDpEndDate) {
		// 新的上一次数据清洗值，根据条件进行设置
		Date newLastDpDatetime = rs_last_dp_datetime;
		
		//============>
		// fix bug by chao.tang@2016-09-23 for 数据缺失 Begin
		if (paramDpStartDate == null && paramDpEndDate == null) {
			newLastDpDatetime = currentSysDatetime;  // now()
		} else {
			if (rs_last_dp_datetime != null) {
				if (paramDpStartDate == null && paramDpEndDate != null) { 
					if (paramDpEndDate.compareTo(newLastDpDatetime) >= 0 ) {
						if (paramDpEndDate.compareTo(currentSysDatetime) >= 0) {
							newLastDpDatetime = currentSysDatetime;  // now()
						} else {
							newLastDpDatetime = paramDpEndDate;
						}
					}
				} else if (paramDpStartDate != null && paramDpEndDate == null) {
					if (paramDpStartDate.compareTo(newLastDpDatetime) <= 0) {
						newLastDpDatetime = currentSysDatetime;  // now()
					}
				} else if (paramDpStartDate != null && paramDpEndDate != null) {
					if (paramDpEndDate.compareTo(newLastDpDatetime) >= 0 && 
							paramDpStartDate.compareTo(newLastDpDatetime) <= 0) {
						//----------------
						if (paramDpEndDate.compareTo(currentSysDatetime) >= 0) {
							newLastDpDatetime = currentSysDatetime;  // now()
						} else {
							newLastDpDatetime = paramDpEndDate;
						}
					}
				}
			} else {
				// 因为上一次同步时间为空，并不知道表中数据的情况，所以此处不能根据 paramDpStartDate 简单的判断
				if (paramDpEndDate != null) { 
					if (paramDpEndDate.compareTo(currentSysDatetime) >= 0) {
						newLastDpDatetime = currentSysDatetime;  // now()
					} else {
						newLastDpDatetime = paramDpEndDate;
					}
				} else {
					newLastDpDatetime = currentSysDatetime;  // now()
				}
			} // rs_last_dp_datetime != null
		}
		// fix bug by chao.tang@2016-09-23 for 数据缺失 End
		
		return newLastDpDatetime;
	}
	
	// 数据批量动态插入
	public int insertDataBySelectRsBatch(DcSysDbConnMapping sysDbConnMapping,
			                             String insertIntoTabColumn,
			                             String selectRsSql,
			                             List<Object> selectArgs,
			                             String destTableOwner,
			                             String destTableName,
			                             List<Object> destTabUniqueConstraintColList,
			                             List<QuerySourceTabColRsEO> inRulesTabColList,
			                             List<QuerySourceTabColRsEO> notInRulesTabColList,
			                             List<QueryTabColumnsRsEO> destTableColList,
			                             boolean initialDataImpFlag) 
		throws ClassNotFoundException, SQLException {
		
		LogManager.appendToLog("Entering insertDataBySelectRsBatch() ---> ");
		
		int returnInsertCount = 0;
		
		// 是否 executeBatch 时 commit
		boolean commitFlag = initialDataImpFlag;
		
		LogManager.appendToLog("commitFlag:" + commitFlag);
		
		//目标数据库的连接
		//if (dcETLHelper == null) {
			DBHelper dcETLHelperDest = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
					    					sysDbConnMapping.getDb_url(), 
					    					sysDbConnMapping.getDb_conn_user(),
					    					sysDbConnMapping.getDb_conn_password(), 
					    					GlobalInfo.MAX_BATCH_SIZE);
		//} else {
			dcETLHelperDest.setBatchSize(GlobalInfo.MAX_BATCH_SIZE);
		//}
		
		List<Object> args = new ArrayList<Object>();
		
		// 获取大记录结果集，单独创建一个connection
		DBHelper dcETLHelperBatch = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
					    					GlobalInfo.DB_DCETL_URL, 
					    					GlobalInfo.DB_DCETL_USER_NAME,
					    					GlobalInfo.DB_DCETL_PASSWORD, 
					    					GlobalInfo.MAX_BATCH_SIZE);
		
		try {
			// execute query
			dcETLHelperBatch.executeQueryBatch(selectRsSql, 
					//TypeConversionUtil.listObjectToObjectArray(selectArgs)
					selectArgs.toArray()
					);
			
			this.logExecuteSqlSummary = this.logExecuteSqlSummary + dcETLHelperBatch.getCurrentErrorSql();
			
			// 获取记录的列数
			ResultSetMetaData rsmd = dcETLHelperBatch.resultSetBatch.getMetaData();
			int selectRsColCount = rsmd.getColumnCount();
			LogManager.appendToLog("selectRsColCount:" + selectRsColCount);
			
			//===============================================
			// 拼接insert into ... values ... 语句
			//===============================================
			StringBuffer question = new StringBuffer("");
			for (int i=1; i<=selectRsColCount; i++) {
				if (question != null && !"".equals(question.toString())){
					question.append("," + "?");
				} else {
					question.append("?");
				}
			}
			
			// 重复记录的处理
			boolean onDuplicateKeyFlag = false;
			StringBuffer onDuplicateKeySql = new StringBuffer("");
			String onDuplicateKeySqlText = "";
			if (destTabUniqueConstraintColList.size() > 0 || inRulesTabColList.size() > 0 || notInRulesTabColList.size() > 0 || destTableColList.size() > 0) {
				onDuplicateKeyFlag = true;
				onDuplicateKeySql.append(" on duplicate key update ");
				// 更新维护的映射值
				for (int i=0; i<inRulesTabColList.size(); i++) {
					onDuplicateKeySql.append(inRulesTabColList.get(i).getDest_tab_column_name() + "=?,");
				}
				// 更新未维护的映射值
				for (int i=0; i<notInRulesTabColList.size();i++){
					onDuplicateKeySql.append(notInRulesTabColList.get(i).getDest_tab_column_name() + "=?,");
				}
				// 如果表都没有维护到规则表，但是存在config表，则走此处的逻辑
				if (inRulesTabColList.size() == 0 && notInRulesTabColList.size() == 0) {
					for (int i=0; i<destTableColList.size();i++){
						onDuplicateKeySql.append(destTableColList.get(i).getColumn_name() + "=?,");
					}
				}
				//取出多余的末尾分号
				onDuplicateKeySqlText = onDuplicateKeySql.substring(0, onDuplicateKeySql.length()-1);
			}
			
			String insertIntoFixedSql = insertIntoTabColumn + " values(" + question.toString() + ")" + onDuplicateKeySqlText;
			LogManager.appendToLog("insertIntoFixedSql:" + insertIntoFixedSql);
			
			int executeBatchCnt = 0;
			//int rsCount = 0;
			//===============================================
			// 循环select的记录
			//===============================================
			dcETLHelperDest.prepareSql(insertIntoFixedSql);
			this.logExecuteSqlSummary = dcETLHelperDest.getCurrentErrorSql() + GlobalInfo.LINE_SEPARATOR + this.logExecuteSqlSummary;
			//LogManager.appendToLog("insertIntoFixedSql:" + insertIntoFixedSql);
			
			LogManager.appendToLog(" >> start fetch resultSetBatch: while (dcETLHelperBatch.resultSetBatch.next()) >>> ", GlobalInfo.DEBUG_MODE);
			
			int loopCount = 0;
			while (dcETLHelperBatch.resultSetBatch.next()) {
				loopCount++;
				//LogManager.appendToLog("currentLoopCount:" + loopCount);
				
				args.clear();
				for (int i=1; i<=selectRsColCount; i++) {
					args.add(dcETLHelperBatch.resultSetBatch.getObject( rsmd.getColumnLabel(i) ));
				}
				
				//重复记录的处理
				if (onDuplicateKeyFlag) {
					// 更新维护的映射值
					for (int i=0; i<inRulesTabColList.size(); i++) {
						args.add(dcETLHelperBatch.resultSetBatch.getObject( inRulesTabColList.get(i).getSource_tab_column_name() ));
					}
					// 更新未维护的映射值
					for (int i=0; i<notInRulesTabColList.size(); i++) {
						args.add(dcETLHelperBatch.resultSetBatch.getObject( notInRulesTabColList.get(i).getSource_tab_column_name() ));
					}
					// 如果表都没有维护到规则表，但是存在config表，则走此处的逻辑
					if (inRulesTabColList.size() == 0 && notInRulesTabColList.size() == 0) {
						for (int i=0; i<destTableColList.size();i++){
							args.add(dcETLHelperBatch.resultSetBatch.getObject( destTableColList.get(i).getColumn_name() ));
						}
					}
				}
				
				dcETLHelperDest.setMSTDataInsertParams(args);
				dcETLHelperDest.addBatch();
				//rsCount++;
				//LogManager.appendToLog("rsCount:" + rsCount);
				
				// 判断是否执行批 20000 一批
				if (dcETLHelperDest.getCurrentSize() == GlobalInfo.MAX_BATCH_SIZE) {
					returnInsertCount = returnInsertCount + dcETLHelperDest.getCurrentSize();
					dcETLHelperDest.exeBatch(0, commitFlag);  // 暂不提交
					dcETLHelperDest.clearBatch();
					executeBatchCnt++;
					LogManager.appendToLog("dcETLHelperDest.exeBatch(" + executeBatchCnt + ") [" + destTableOwner + "." + destTableName + "] => " + 
							TypeConversionUtil.dateToString(new Date()), GlobalInfo.DEBUG_MODE);
				}
				
			} // while
			
			LogManager.appendToLog(" >> end fetch resultSetBatch: while (dcETLHelperBatch.resultSetBatch.next()) <<< ", GlobalInfo.DEBUG_MODE);
			
			if (dcETLHelperDest.getCurrentSize() > 0 ){
				returnInsertCount = returnInsertCount + dcETLHelperDest.getCurrentSize();
				dcETLHelperDest.exeBatch(99, commitFlag);  // 暂不提交
			}
			
			LogManager.appendToLog("executeSqlSummary:" + this.logExecuteSqlSummary, GlobalInfo.STATEMENT_MODE);
			LogManager.appendToLog("returnInsertCount:" + returnInsertCount);
			
		} catch (Exception e) {
			dcETLHelperDest.rollback();  // 回滚未提交的事务
			e.printStackTrace();
			String currentErrorMsg = " *** insertDataBySelectRsBatch() [" + destTableOwner + "."+ destTableName + 
						"] => Exception: " + e.toString() + ", 错误时正在执行的SQL:[" + dcETLHelperDest.getCurrentErrorSql() + 
						"] => [" + dcETLHelperBatch.getCurrentErrorSql() + "]";
					
			LogManager.appendToLog(currentErrorMsg, GlobalInfo.EXCEPTION_MODE);
			throw new RuntimeException(currentErrorMsg);
		} finally {
			dcETLHelperDest.clearBatch();
			dcETLHelperDest.closeAll();
			//大记录集连接对象关闭
			dcETLHelperBatch.closeAll();
		}
		
		LogManager.appendToLog("Leaving insertDataBySelectRsBatch() ---> ");
		
		return returnInsertCount;
	}
	
	/**********************************
	 * 数据清理主程序
	 * 
	 * @param ifaceParameter
	 * @param actionFlag
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException
	 * @throws ParseException
	 **********************************/
	public IfaceReturns startDataPurgeEngine(IfaceParameters ifaceParameter,
											 String actionFlag) 
            throws ClassNotFoundException, SQLException, IOException, ParseException {
		
		IfaceReturns ifaceReturn = new IfaceReturns();
		
		// 获取datapurge列表清单
		List<QueryDPInstanceConfigsRsEO> dpTabList= getDataPurgeTabLists(ifaceParameter.getSourceSysKey(),
				  														 ifaceParameter.getSourceTabOwner(), 
				  														 ifaceParameter.getSourceTabName());
		if (dpTabList.size() == 0 ) {
			ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			ifaceReturn.setReturnMsg("根据参数（source_sys_key:" + ifaceParameter.getSourceSysKey() + 
					" source_table_owner:" + ifaceParameter.getSourceTabOwner() + 
					" source_table_name:" + ifaceParameter.getSourceTabName() + 
					"） 未找到需要进行数据清洗的表清单，请检查相关配置.");
			return ifaceReturn;
		}
		
		boolean initDataImpFlag = false;
		if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
			initDataImpFlag=true;
			// 期初标识参数=Y 或输入的起始和截止日期大于7天
			/*
			if ( (ifaceParameter.getInitialDataImpFlag() != null && "Y".equals(ifaceParameter.getInitialDataImpFlag())) || 
				 (ifaceParameter.getDpStartDate() != null && ifaceParameter.getDpEndDate() != null && 
				  DateOperationUtil.daysBetween(ifaceParameter.getDpStartDate(), ifaceParameter.getDpEndDate()) > GlobalInfo.MAX_RECORDS_INTERVAL_DAYS) || 
				 (ifaceParameter.getDpStartDate() != null && ifaceParameter.getDpEndDate() == null && 
				  DateOperationUtil.daysBetween(ifaceParameter.getDpStartDate(), new Date()) > GlobalInfo.MAX_RECORDS_INTERVAL_DAYS)
				) {
				initDataImpFlag = true;
			}*/
		}
		
		//
		//初始化分库分表数据库连接信息实体
		DcSysDbConnMapping sysDbConnMapping = null;
		StringBuffer logFileContents = new StringBuffer("");
		
		
		for (QueryDPInstanceConfigsRsEO rs : dpTabList) {
			
			LogManager.appendToLog("======================================================", 
					GlobalInfo.STATEMENT_MODE);
			
			logFileContents.delete(0, logFileContents.length());  // clean 日志信息
			List<Object> args = new ArrayList<Object>();
			String sourceDpTableName = rs.getSource_dp_table_name();  // 清洗的源表
			String concurrentProcessingId = null;  // 并发控制表ID
			
			LogManager.appendToLog("Start process data purge: [" + rs.getSource_dp_table_owner() + "." + 
					sourceDpTableName + "] --- ");
			
			// 判断连接是否已经创建？
			if (dcETLHelper == null) {
				/*DBHelper*/ dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
												GlobalInfo.DB_DCETL_URL, 
												GlobalInfo.DB_DCETL_USER_NAME,
												GlobalInfo.DB_DCETL_PASSWORD, 
												null);
			}
			
			String startDpDate = TypeConversionUtil.dateToString( new Date() );
			LogManager.appendToLog("  (1)->Run time [startDpDate:" + startDpDate + "]");
			
			//===========================
			// 0. 检查当前清洗的表是否存在启用的规则 但 未验证通过
			//===========================
			boolean isValidDpRulesFlag = checkIsValidDpRules(rs.getSource_sys_key(),
	                 										 rs.getSource_dp_table_owner(),
	                 										 sourceDpTableName,
	                 										 rs.getDest_dp_table_owner(),
	                 										 rs.getDest_dp_table_name());
			logFileContents.append("isValidDpRulesFlag:[" + isValidDpRulesFlag + "]~");
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) && !isValidDpRulesFlag) {
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg("数据清洗规则维护的来源" + rs.getSource_sys_key() + "表 " + 
										 rs.getSource_dp_table_owner() + "." + 
						                 sourceDpTableName + " => " + rs.getDest_dp_table_owner() + "." + 
						                 rs.getDest_dp_table_name() + 
						                 " 有效但验证未通过，请先验证该规则相关的SQL有效后再继续" + "; " + 
						                 ifaceReturn.getReturnMsg());
				
				// 插入错误日志
				args.clear();
				args.add(GeneratorUUID.generateUUID());
				args.add(actionFlag);
				args.add(ifaceParameter.getSourceSysKey());
				args.add(rs.getSource_dp_table_owner());
				args.add(sourceDpTableName);
				args.add(rs.getDest_dp_table_owner());
				args.add(rs.getDest_dp_table_name());
				args.add(startDpDate);
				args.add(TypeConversionUtil.dateToString( new Date() ));
				args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
				args.add(ifaceReturn.getReturnMsg());  // process_message
				args.add(dcETLHelper.getCurrentErrorSql());
				args.add(logFileContents.toString());  // log_file
				args.add(null);
				args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
				args.add(ifaceParameter.getSourceTabOwner());
				args.add(ifaceParameter.getSourceTabName());
				args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
				args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
				args.add(null);  // process_row_count
				
				// 采用异步线程的方式插入
				if (insertDpInstanceLog(dcETLHelper, args)) {
				}
				
				continue;  // 继续下一个表的处理
			}
			
			//===========================
			// 并发控制
			//===========================
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
				concurrentProcessingId = 
						new ConcurrentControlManager(rs.getSource_sys_key(),
								rs.getSource_dp_table_owner(),
								sourceDpTableName,
								rs.getDest_dp_table_owner(),
								rs.getDest_dp_table_name()).isConcurrentDpProcessing();
				logFileContents.append("concurrentProcessingId:[" + concurrentProcessingId + "]~");
				if (concurrentProcessingId == null) {
					ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
					ifaceReturn.setReturnMsg("数据清洗来源" + rs.getSource_sys_key() + "表 " + 
										     rs.getSource_dp_table_owner() + "." + 
										     sourceDpTableName + " => " + rs.getDest_dp_table_owner() + "." + 
										     rs.getDest_dp_table_name() + 
										     " 处理中，请稍后在处理" + "; " + 
										     ifaceReturn.getReturnMsg());
							
					// 插入错误日志
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(actionFlag);
					args.add(ifaceParameter.getSourceSysKey());
					args.add(rs.getSource_dp_table_owner());
					args.add(sourceDpTableName);
					args.add(rs.getDest_dp_table_owner());
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(TypeConversionUtil.dateToString( new Date() ));
					args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
					args.add(ifaceReturn.getReturnMsg());  // process_message
					args.add("");
					args.add(logFileContents.toString());  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					args.add(null);  // process_row_count
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
					continue;  // 继续下一个表的处理
				}
			}
			
			// 上一次同步时间为空，说明应该是数据初始化
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) && 
					( (rs.getLast_dp_datetime() != null && DateOperationUtil.daysBetween(rs.getLast_dp_datetime(), new Date()) > GlobalInfo.MAX_RECORDS_INTERVAL_DAYS) || 
						rs.getLast_dp_datetime() == null) ) {
				initDataImpFlag = true;
			}
			
			//===========================
			// 1. 获取维护了规则的表列信息（包括规则列表中的列 & 非规则列表维护的列）
			//===========================
			List<QuerySourceTabColRsEO> inRulesTabColList = 
					getDataPurgeTabColumnLists(ifaceParameter.getSourceSysKey(),
						 					   rs.getSource_dp_table_owner(), 
						 					   sourceDpTableName,
						 					   actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) ? "Y" : "N",  // Modified by chao.tang@2016-07-06 for 解决验证和数据清洗的条件过来bug
						 					   true);
			String getInRulesTabColSqlText = dcETLHelper.getCurrentErrorSql();  // 获取执行的sql
			List<QuerySourceTabColRsEO> notInRulesTabColList = 
					getDataPurgeTabColumnLists(ifaceParameter.getSourceSysKey(),
						 					   rs.getSource_dp_table_owner(), 
						 					   sourceDpTableName,
						 					   actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) ? "Y" : "N",  // Modified by chao.tang@2016-07-06 for 解决验证和数据清洗的条件过来bug
						 					   false);
			String getNotInRulesTabColSqlText = dcETLHelper.getCurrentErrorSql();  // 获取执行的sql
			if (inRulesTabColList.size() > 0) {
				logFileContents.append("inRulesTabColList.getDp_rules_header_id:[" + inRulesTabColList.get(0).getDp_rules_header_id() + "]~");
			} else if (notInRulesTabColList.size() > 0) {
				logFileContents.append("notInRulesTabColList.getDp_rules_header_id:[" + notInRulesTabColList.get(0).getDp_rules_header_id() + "]~");
			}
			logFileContents.append("inRulesTabColList.size:[" + inRulesTabColList.size() + "]~");
			logFileContents.append("notInRulesTabColList.size:[" + notInRulesTabColList.size() + "]~");
			
			LogManager.appendToLog("  inRulesTabColList.size => " + inRulesTabColList.size() + GlobalInfo.LINE_SEPARATOR + getInRulesTabColSqlText);
			LogManager.appendToLog("  notInRulesTabColList.size => " + notInRulesTabColList.size() + GlobalInfo.LINE_SEPARATOR + getNotInRulesTabColSqlText);
			
			//获取规则头ID
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
				String dpRulesHeaderId = getDpRulesHeaderId(ifaceParameter.getSourceSysKey(),
						   									rs.getSource_dp_table_owner(), 
						   									sourceDpTableName);
				logFileContents.append("dpRulesHeaderId:[" + dpRulesHeaderId + "]~");
				logFileContents.append("rs.getDp_rules_header_id():[" + rs.getDp_rules_header_id() + "]");
				
				if ( (dpRulesHeaderId != null && !"".equals(dpRulesHeaderId)) || (rs.getDp_rules_header_id() != null && !"".equals(rs.getDp_rules_header_id())) ) {
					boolean raiseRulesErrFlag = false;
					if (inRulesTabColList.size() == 0 || notInRulesTabColList.size() == 0) {
						raiseRulesErrFlag = true;
					}
					if ( (dpRulesHeaderId == null && rs.getDp_rules_header_id() != null) || 
						 (dpRulesHeaderId != null && rs.getDp_rules_header_id() == null) ||
						 (dpRulesHeaderId != null && rs.getDp_rules_header_id() != null && !dpRulesHeaderId.equals(rs.getDp_rules_header_id())) ) {
						raiseRulesErrFlag = true;
					}
					//单独获取规则能获取到，再次判断规则是否和上述获取到的规则列一致?
					if (raiseRulesErrFlag) {
						ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
						ifaceReturn.setReturnMsg("dpRulesHeaderId:[" + dpRulesHeaderId + "]~ rs.getDp_rules_header_id():[" + rs.getDp_rules_header_id() + "]~ But " + 
												 rs.getSource_dp_table_owner() + "." + sourceDpTableName + 
								                 " cannot find any valid columns, please check and retry again" + "; " + ifaceReturn.getReturnMsg());
						
						// Log for debug
						printDebugStackInfo(rs.getSource_sys_key(),
					 			rs.getSource_dp_table_owner(),
					 			sourceDpTableName,
					 			rs.getDest_dp_table_owner(),
					 			rs.getDest_dp_table_name(),
					 			rs.getDp_rules_header_id());
						
						// 插入错误日志
						if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
							args.clear();
							args.add(GeneratorUUID.generateUUID());
							args.add(actionFlag);
							args.add(ifaceParameter.getSourceSysKey());
							args.add(rs.getSource_dp_table_owner());
							args.add(sourceDpTableName);
							args.add(rs.getDest_dp_table_owner());
							args.add(rs.getDest_dp_table_name());
							args.add(startDpDate);
							args.add(TypeConversionUtil.dateToString( new Date() ));
							args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
							args.add(ifaceReturn.getReturnMsg());  // process_message
							args.add(dcETLHelper.getCurrentErrorSql() + " *** " + getInRulesTabColSqlText + " *** " + getNotInRulesTabColSqlText);
							args.add(logFileContents.toString());  // log_file
							args.add(null);
							args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
							args.add(ifaceParameter.getSourceTabOwner());
							args.add(ifaceParameter.getSourceTabName());
							args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
							args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
							args.add(null);  // process_row_count
							
							// 采用异步线程的方式插入
							if (insertDpInstanceLog(dcETLHelper, args)) {
							}
						}
						
						// 并发控制
						if (concurrentProcessingId != null) {
						  new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
						}
						
						continue;
					}
				}
			}
			
			//===========================
			// 2. 获取目标表的所有列
			//===========================
			List<QueryTabColumnsRsEO> destTableColList = 
					getDataPurgeTabColumnListsOf(rs.getDest_dp_table_owner(), 
						 					     rs.getDest_dp_table_name());
			LogManager.appendToLog("  destTableColList.size => " + destTableColList.size());
			if (destTableColList.size() == 0) {
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg(rs.getDest_dp_table_owner() + "." + rs.getDest_dp_table_name() + 
						                 " cannot find any valid columns, please check and retry again" + "; " + ifaceReturn.getReturnMsg());
				
				// 插入错误日志
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(actionFlag);
					args.add(ifaceParameter.getSourceSysKey());
					args.add(rs.getSource_dp_table_owner());
					args.add(sourceDpTableName);
					args.add(rs.getDest_dp_table_owner());
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(TypeConversionUtil.dateToString( new Date() ));
					args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
					args.add(ifaceReturn.getReturnMsg());  // process_message
					args.add(dcETLHelper.getCurrentErrorSql());
					args.add(logFileContents.toString());  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					args.add(null);  // process_row_count
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
				}
				
				// 并发控制
				if (concurrentProcessingId != null) {
				  new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
				}
				
				continue;
			}
			
			//===========================
			// 3. 获取目标表的主键列
			//===========================
			List<Object> destTabUniqueConstraintColList = getTabUniqueConstraintCol(rs.getDest_dp_table_owner(), 
				     													            rs.getDest_dp_table_name());
			LogManager.appendToLog("  destTabUniqueConstraintColList.size => " + 
				     						destTabUniqueConstraintColList.size());
			
			//===========================
			// 4. 存放 SQL拼接结果
			//===========================
			StringBuffer selectFromColumnList = new StringBuffer("");
			StringBuffer insertIntoColumnList = new StringBuffer("");
			StringBuffer onDuplicateKeyUptList = new StringBuffer(""); 
			String notExistsSql	= "";
			String leftJoinClause = "";
			String sourTabOwner	= rs.getSource_dp_table_owner();
			String sourFromTabName = rs.getSource_dp_table_name();
			String sourTabWhereClause = "(1=1)";
			String destTabOwner = rs.getDest_dp_table_owner();
			String destTabName = rs.getDest_dp_table_name();
			String fromConditionSourTabAlias = GlobalInfo.SOUR_TAB_ALIAS_NAME;  // 标的别名，用于替换
			
			if (inRulesTabColList.size() == 0 && notInRulesTabColList.size() == 0) {
				// 没有维护规则的表，但是在 dp_data_purge_inst_configs 表中存在
				List<QueryTabColumnsRsEO> tableColList = 
						getDataPurgeTabColumnListsOf(rs.getSource_dp_table_owner(), 
							 					     sourceDpTableName);
				LogManager.appendToLog("  [###]tableColList.size => " + tableColList.size());
				
				if (tableColList.size() > 0) {
					for (int i=0; i<tableColList.size(); i++) {
						// 判断目标表是否存在该列，进行列匹配插入
						if ( isTabColumnExists(/*rs.getDest_dp_table_owner(),
											   rs.getDest_dp_table_name(),*/
											   destTableColList,
										       tableColList.get(i).getColumn_name()) ) {
							if (selectFromColumnList != null && !"".equals(selectFromColumnList.toString())) {
								selectFromColumnList.append("," + fromConditionSourTabAlias + "." + tableColList.get(i).getColumn_name());
							} else {
								selectFromColumnList.append(fromConditionSourTabAlias + "." + tableColList.get(i).getColumn_name());
							}
							if (insertIntoColumnList != null && !"".equals(insertIntoColumnList.toString())) {
								insertIntoColumnList.append("," + tableColList.get(i).getColumn_name());
							} else {
								insertIntoColumnList.append(tableColList.get(i).getColumn_name());
							}
						}
					} // for i 
				} else {
					ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
					ifaceReturn.setReturnMsg(rs.getSource_dp_table_owner() + "." + sourceDpTableName + 
							                 " cannot find any valid columns, please check and retry again" + "; " + 
							                 ifaceReturn.getReturnMsg());
					
					// 插入错误日志
					if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
						args.clear();
						args.add(GeneratorUUID.generateUUID());
						args.add(actionFlag);
						args.add(ifaceParameter.getSourceSysKey());
						args.add(rs.getSource_dp_table_owner());
						args.add(sourceDpTableName);
						args.add(rs.getDest_dp_table_owner());
						args.add(rs.getDest_dp_table_name());
						args.add(startDpDate);
						args.add(TypeConversionUtil.dateToString( new Date() ));
						args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
						args.add(ifaceReturn.getReturnMsg());  // process_message
						args.add("");
						args.add(logFileContents.toString());  // log_file
						args.add(null);
						args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
						args.add(ifaceParameter.getSourceTabOwner());
						args.add(ifaceParameter.getSourceTabName());
						args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
						args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
						args.add(null);  // process_row_count
						
						// 采用异步线程的方式插入
						if (insertDpInstanceLog(dcETLHelper, args)) {
						}
					}
					
					// 并发控制
					if (concurrentProcessingId != null) {
					  new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
					}
					
					continue;
					
				}  // if tableColList.size() > 0
				
			} else {
				LogManager.appendToLog("  Build dynamic sql according to rules --->", GlobalInfo.STATEMENT_MODE);
				// 根据 规则进行动态拼接列汇总信息
				for (int i=0; i<notInRulesTabColList.size(); i++) {
					// 如果该字段为空格并不为空，也要过滤掉
					if (notInRulesTabColList.get(i).getFrom_condition_tab_list().trim() != null && 
							!"".equals(notInRulesTabColList.get(i).getFrom_condition_tab_list().trim())) {
						sourFromTabName = notInRulesTabColList.get(i).getFrom_condition_tab_list().trim();
					}
					fromConditionSourTabAlias = getFromConditionSourTabAlias(sourFromTabName, sourceDpTableName);
					
					if (notInRulesTabColList.get(i).getWhere_condition_sql() != null &&
						!"".equals(notInRulesTabColList.get(i).getWhere_condition_sql())) {
						sourTabWhereClause = notInRulesTabColList.get(i).getWhere_condition_sql().trim();
					}
					
					// 判断目标表是否存在该列，进行列匹配插入
					if ( isTabColumnExists(/*rs.getDest_dp_table_owner(),
										   rs.getDest_dp_table_name(),*/
										   destTableColList,
										   notInRulesTabColList.get(i).getSource_tab_column_name()) ) {
						if (selectFromColumnList != null && !"".equals(selectFromColumnList.toString())) {
							selectFromColumnList.append("," + 
											appendSourTabAlias(destTableColList, 
													sourceDpTableName + "." + notInRulesTabColList.get(i).getSource_tab_column_name(),
													sourceDpTableName,
													fromConditionSourTabAlias));
						} else {
							selectFromColumnList.append(appendSourTabAlias(destTableColList, 
															sourceDpTableName + "." +  notInRulesTabColList.get(i).getSource_tab_column_name(),
															sourceDpTableName,
															fromConditionSourTabAlias));
						}
						
						// 为了便于处理，赋予一个别名
						selectFromColumnList.append(" AS " + notInRulesTabColList.get(i).getSource_tab_column_name());
						
						if (insertIntoColumnList != null && !"".equals(insertIntoColumnList.toString())) {
							insertIntoColumnList.append("," + notInRulesTabColList.get(i).getDest_tab_column_name());
						} else {
							insertIntoColumnList.append(notInRulesTabColList.get(i).getDest_tab_column_name());
						}
						
						// 不存在规则的话，亦更新规则的列值
						if (!"".equals(onDuplicateKeyUptList.toString()) && onDuplicateKeyUptList != null) {
							onDuplicateKeyUptList.append(", " + 
									notInRulesTabColList.get(i).getDest_tab_column_name() + 
									"=" + 
									appendSourTabAlias(destTableColList, 
											sourceDpTableName + "." + notInRulesTabColList.get(i).getDp_return_column_val(),
											sourceDpTableName,
											fromConditionSourTabAlias));
						} else {
							onDuplicateKeyUptList.append(notInRulesTabColList.get(i).getDest_tab_column_name() + 
									"=" + 
									appendSourTabAlias(destTableColList, 
											sourceDpTableName + "." + notInRulesTabColList.get(i).getDp_return_column_val(),
											sourceDpTableName,
											fromConditionSourTabAlias));
						}
						LogManager.appendToLog("  notInRulesTabColList<<onDuplicateKeyUptList>>: " + onDuplicateKeyUptList.toString(), 
								GlobalInfo.STATEMENT_MODE);
						
					}
				} // for i 
				//////////////////////////////////////////////////////////////
				for (int j=0; j<inRulesTabColList.size(); j++) {
					
					// 如果该字段为空格并不为空，也要过滤掉
					if (inRulesTabColList.get(j).getFrom_condition_tab_list().trim() != null && 
							!"".equals(inRulesTabColList.get(j).getFrom_condition_tab_list().trim())) {
						sourFromTabName = inRulesTabColList.get(j).getFrom_condition_tab_list().trim();
					}
					fromConditionSourTabAlias = getFromConditionSourTabAlias(sourFromTabName, sourceDpTableName);
					
					if (inRulesTabColList.get(j).getWhere_condition_sql() != null &&
						!"".equals(inRulesTabColList.get(j).getWhere_condition_sql())) {
						sourTabWhereClause = inRulesTabColList.get(j).getWhere_condition_sql().trim();
					}
					
					// 判断目标表是否存在该列，进行列匹配插入
					if ( isTabColumnExists(/*rs.getDest_dp_table_owner(),
										   rs.getDest_dp_table_name(),*/
										   destTableColList,
										   inRulesTabColList.get(j).getSource_tab_column_name()) ) {
						if (selectFromColumnList != null && !"".equals(selectFromColumnList.toString())) {
							selectFromColumnList.append("," + 
										appendSourTabAlias(destTableColList, 
												inRulesTabColList.get(j).getDp_return_column_val(),
												sourceDpTableName,
												fromConditionSourTabAlias));
						} else {
							selectFromColumnList.append(appendSourTabAlias(destTableColList, 
															inRulesTabColList.get(j).getDp_return_column_val(),
															sourceDpTableName,
															fromConditionSourTabAlias));
						}
						
						// 为了便于处理，赋予一个别名
						selectFromColumnList.append(" AS " + inRulesTabColList.get(j).getSource_tab_column_name());
						
						if (insertIntoColumnList != null && !"".equals(insertIntoColumnList.toString())) {
							insertIntoColumnList.append("," + inRulesTabColList.get(j).getDest_tab_column_name());
						} else {
							insertIntoColumnList.append(inRulesTabColList.get(j).getDest_tab_column_name());
						}
						
						// 存在规则的话，则更新规则的列值
						if (!"".equals(onDuplicateKeyUptList.toString()) && onDuplicateKeyUptList != null) {
							onDuplicateKeyUptList.append(", " + 
									inRulesTabColList.get(j).getDest_tab_column_name() + 
									"=" + 
									appendSourTabAlias(destTableColList, 
											inRulesTabColList.get(j).getDp_return_column_val(),
											sourceDpTableName,
											fromConditionSourTabAlias));
						} else {
							onDuplicateKeyUptList.append(inRulesTabColList.get(j).getDest_tab_column_name() + 
									"=" + 
									appendSourTabAlias(destTableColList, 
											inRulesTabColList.get(j).getDp_return_column_val(),
											sourceDpTableName,
											fromConditionSourTabAlias));
						}
						LogManager.appendToLog("  inRulesTabColList<<onDuplicateKeyUptList>>: " + onDuplicateKeyUptList.toString(), 
								GlobalInfo.STATEMENT_MODE);
						
					}
				} // for j 
			}
			
			
			//*************************************************
			//===========================
			// 5. 进行sql拼接
			//===========================
			LogManager.appendToLog("  insertIntoColumnList: " + insertIntoColumnList.toString(), GlobalInfo.STATEMENT_MODE);
			LogManager.appendToLog("  selectFromColumnList: " + selectFromColumnList.toString(), GlobalInfo.STATEMENT_MODE);
			LogManager.appendToLog("  onDuplicateKeyUptList: " + onDuplicateKeyUptList.toString(), GlobalInfo.STATEMENT_MODE);
			if (insertIntoColumnList.toString().length() == 0 || selectFromColumnList.toString().length() == 0) {
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg(rs.getDest_dp_table_owner() + "." + rs.getDest_dp_table_name() + 
						                 " cannot find any columns" + "; " + ifaceReturn.getReturnMsg());
				
				// 插入错误日志
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(actionFlag);
					args.add(ifaceParameter.getSourceSysKey());
					args.add(rs.getSource_dp_table_owner());
					args.add(sourceDpTableName);
					args.add(rs.getDest_dp_table_owner());
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(TypeConversionUtil.dateToString( new Date() ));
					args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
					args.add(ifaceReturn.getReturnMsg());  // process_message
					args.add("");
					args.add(logFileContents.toString());  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					args.add(null);  // process_row_count
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
				}
				
				// 并发控制
				if (concurrentProcessingId != null) {
				  new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
				}
				
				continue;
			}
			
			// not exists 替换为 left join的方式，提高效率
			// Fix bug for 外链接，增加if判断逻辑，数据清洗时不采用外链接的方式进行数据过滤
			if (!actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
				if (inRulesTabColList.size() == 0) {
					leftJoinClause = " LEFT JOIN " + destTabOwner + "." + destTabName + " " + 
							GlobalInfo.DEST_TAB_ALIAS_NAME + " ON (";
					
					String connectWhereTemp = null;
					for (int i=0; i<destTabUniqueConstraintColList.size(); i++) {
						if (connectWhereTemp != null && !"".equals(connectWhereTemp)) {
							//notExistsSql = notExistsSql + " and " + GlobalInfo.DEST_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString() + 
							//		" IS NULL ";
							connectWhereTemp = connectWhereTemp + " and " + 
									GlobalInfo.DEST_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString() + 
									"=" + 
									GlobalInfo.SOUR_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString();
						} else {
							notExistsSql = GlobalInfo.DEST_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString() + 
									" IS NULL ";
							connectWhereTemp = GlobalInfo.DEST_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString() + 
									"=" + 
									GlobalInfo.SOUR_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString();
						}
					} // for
					if (connectWhereTemp == null) {
						connectWhereTemp = "1=1";
					}
					
					leftJoinClause = leftJoinClause + connectWhereTemp + ") ";
				}
			}
			
			//leftJoinClause = "";  /// ?????????????
			
			
			//===========================
			// 分库分表逻辑，根据source_sys_key获取数据库连接
			//===========================
			try {
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
					sysDbConnMapping = DbConnMappingUtil.initDbConnMapping(dcETLHelper, rs.getSource_sys_key());
					logFileContents.append("sysDbConnMapping.getDb_schema():[" + sysDbConnMapping.getDb_schema() + "]~");
				}
			} catch (Exception e) {
				//e.printStackTrace();
				LogManager.appendToLog("DataPurgeETL --> DbConnMappingUtil.initDbConnMapping() Exception:" + e.toString(),
						GlobalInfo.EXCEPTION_MODE);
				//
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg("未找到source_sys_key=" + rs.getSource_sys_key() + " 目标数据库的连接配置信息，请检查表dc_sys_db_conn_mapping的配置. " + 
						"; " + ifaceReturn.getReturnMsg());
				
				// 插入错误日志
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(actionFlag);
					args.add(ifaceParameter.getSourceSysKey());
					args.add(rs.getSource_dp_table_owner());
					args.add(sourceDpTableName);
					args.add(rs.getDest_dp_table_owner());
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(TypeConversionUtil.dateToString( new Date() ));
					args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
					args.add(ifaceReturn.getReturnMsg());  // process_message
					args.add("");
					args.add(logFileContents.toString());  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					args.add(null);  // process_row_count
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
				}
				
				// 并发控制
				if (concurrentProcessingId != null) {
				  new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
				}
				
				// 数据库连接都不存在，直接返回
				return ifaceReturn;
			}
			
			
			//===========================
			// 构造各个部分的sql
			//===========================
			String selectSql = generateSourceQuerySql(ifaceParameter, 
					sourTabOwner, 
					sourFromTabName, 
					selectFromColumnList.toString(),   // 多表连接，需要进行制定列名前缀
					sourTabWhereClause,
					leftJoinClause);
			String insertIntoSql = generateDestInsertSql(ifaceParameter, 
					(actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) && sysDbConnMapping != null)?sysDbConnMapping.getDb_schema():destTabOwner,  //分库分表
					destTabName, 
					insertIntoColumnList.toString());
			LogManager.appendToLog("  selectSql: " + selectSql, GlobalInfo.STATEMENT_MODE);
			LogManager.appendToLog("  insertIntoSql: " + insertIntoSql, GlobalInfo.STATEMENT_MODE);
			
			// 最终的组装
			StringBuffer fullDataPurgeSql = null;
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) && initDataImpFlag) {
				// 期初数据导入则：改变方式
				fullDataPurgeSql = new StringBuffer(selectSql);
			} else {
				fullDataPurgeSql = new StringBuffer(insertIntoSql + GlobalInfo.LINE_SEPARATOR + selectSql);
			}
			
			boolean startDateFilterConditFlag = false;
			boolean endDateFilterConditFlag = false;
			boolean dp_tab_filter_condit3_flag = false;
			@SuppressWarnings("unused")
			boolean dp_tab_filter_condit4_flag = false;
			@SuppressWarnings("unused")
			boolean dp_tab_filter_condit5_flag = false;
			int filterConditCount = 0;
			
			// 第一个字段表示dpStartDate
			if (rs.getSource_dp_tab_filter_condit1() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit1())) {
				fullDataPurgeSql.append(" and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit1(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")");
				startDateFilterConditFlag = true;
				filterConditCount = filterConditCount + 1;
			}
			// 第二个字段表示dpEndDate
			if (rs.getSource_dp_tab_filter_condit2() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit2())) {
				fullDataPurgeSql.append(" and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit2(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")");
				endDateFilterConditFlag = true;
				filterConditCount = filterConditCount + 1;
			}
			// source_sys_key
			if (rs.getSource_dp_tab_filter_condit3() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit3())) {
				fullDataPurgeSql.append(" and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit3(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")");
				dp_tab_filter_condit3_flag = true;
				filterConditCount = filterConditCount + 1;
			}
			// 后面的字段4,5暂不替换
			if (rs.getSource_dp_tab_filter_condit4() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit4())) {
				fullDataPurgeSql.append(" and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit4(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")");
				dp_tab_filter_condit4_flag = true;
				filterConditCount = filterConditCount + 1;
			}
			if (rs.getSource_dp_tab_filter_condit5() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit5())) {
				fullDataPurgeSql.append(" and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit5(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")");
				dp_tab_filter_condit5_flag = true;
				filterConditCount = filterConditCount + 1;
			}
			
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_VALIDATION_RULES)) {
				fullDataPurgeSql.append(" and (1=2) ");
			}
			
			// on duplicate key update 判断
			if ((inRulesTabColList.size() > 0 || notInRulesTabColList.size() > 0) && !initDataImpFlag) {
				if (onDuplicateKeyUptList != null && !"".equals(onDuplicateKeyUptList.toString())) {
					fullDataPurgeSql.append(GlobalInfo.LINE_SEPARATOR + 
							" on duplicate key update " + onDuplicateKeyUptList.toString());
				}
			} else {
				//notExistsSql = "";  //　?????????????
				
				if (notExistsSql != null && !"".equals(notExistsSql)) {
					fullDataPurgeSql.append(GlobalInfo.LINE_SEPARATOR + " and (" + notExistsSql + ")");
				}
			}
			
			LogManager.appendToLog("  *** fullDataPurgeSql: " + fullDataPurgeSql.toString());
			
			//===========================
			// 6. 执行 数据清洗脚本 & 更新配置表的上一次数据清洗时间 + 插入数据清洗log日志表
			//===========================
			// 此次清洗到当前时间节点
			Date currentSysDatetime = new java.sql.Date(new Date().getTime());  // get current sysdate
			// Fix bug @2016-09-27 for 时间同步不一致问题 Begin
			dcETLHelper.executeQuery(GlobalSql.QUERY_DB_NOW_DATE, null);
			if (dcETLHelper.resultSet.next()) {
				currentSysDatetime =  TypeConversionUtil.dateCheck(dcETLHelper.resultSet.getDate("DB_SYSDATE").toString() + " " + 
						dcETLHelper.resultSet.getTime("DB_SYSDATE").toString());
			}
			// Fix bug @2016-09-27 for 时间同步不一致问题 Begin
			
			
			// fix bug by chao.tang@2016-09-23 for 数据缺失 Begin
			// 新的上一次数据清洗值，根据条件进行设置
			Date newLastDpDatetime = getNewLastDpDatetime(rs.getLast_dp_datetime(),
														  currentSysDatetime,
														  ifaceParameter.getDpStartDate(),
														  ifaceParameter.getDpEndDate());
			// 设置取数参数
			args.clear();
			if (startDateFilterConditFlag) {
				args.add(ifaceParameter.getDpStartDate());
				args.add(rs.getLast_dp_datetime());
				args.add(rs.getDp_tolerance_seconds());
				LogManager.appendToLog("rs.getDp_tolerance_seconds():**********:" + rs.getDp_tolerance_seconds());
			}
			if (endDateFilterConditFlag) {
				if (ifaceParameter.getDpEndDate() != null) {
					args.add(ifaceParameter.getDpEndDate());
				} else {
					args.add(newLastDpDatetime);
				}
			}
			// set source_sys_key
			if (dp_tab_filter_condit3_flag) {
				args.add(ifaceParameter.getSourceSysKey());
			}
			// fix bug by chao.tang@2016-09-23 for 数据缺失 End
			
			
			boolean exceptionFlag = false;
			int insertCount = 0;
			
			try {
				// <<execute query>>
				LogManager.appendToLog("  (2)->begin execute fullDataPurgeSql ", GlobalInfo.DEBUG_MODE);
				LogManager.appendToLog("    initDataImpFlag:" + initDataImpFlag, GlobalInfo.DEBUG_MODE);
				
				String fullDataPurgeSqlBak = fullDataPurgeSql.toString();
				this.logExecuteSqlSummary = new String("");  // 记录完整的执行sql
				if ( !initDataImpFlag ) {
					// 验证保持原来的逻辑
					dcETLHelper.setAndExecuteDML(fullDataPurgeSql.toString(), args);
					dcETLHelper.close();
					//
					if (dcETLHelper.getCurrentErrorSql() != null && 
							!"".equals(dcETLHelper.getCurrentErrorSql())) {
						fullDataPurgeSqlBak = dcETLHelper.getCurrentErrorSql();
					}
				} else {
					insertCount = insertDataBySelectRsBatch(sysDbConnMapping, insertIntoSql, fullDataPurgeSql.toString(), args, 
							destTabOwner, destTabName, destTabUniqueConstraintColList, 
							inRulesTabColList, 
							notInRulesTabColList,
							destTableColList,
							initDataImpFlag);
					fullDataPurgeSqlBak = this.logExecuteSqlSummary;
					LogManager.appendToLog("  insertCount:" + insertCount, GlobalInfo.STATEMENT_MODE);
					LogManager.appendToLog("  fullDataPurgeSqlBak:" + fullDataPurgeSqlBak, GlobalInfo.STATEMENT_MODE);
				}
				LogManager.appendToLog("  (2)->end execute fullDataPurgeSql ", GlobalInfo.DEBUG_MODE);
				
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
					//=============================
					// <<更新配置表的上一次数据清洗时间>>
					//=============================
					LogManager.appendToLog("  (3)->Begin update/insert data purge instance config autoc LAST_DP_DATETIME");
					
					String dateMode = GlobalInfo.OTHERS_DATE_VAL_MODE;
					LogManager.appendToLog("  newLastDpDatetime: [" + dateMode + "]" + TypeConversionUtil.dateToString(newLastDpDatetime));
					
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(rs.getDp_inst_config_id());
					args.add(rs.getSource_sys_key());
					args.add(null);  // SOUR_SYS_DP_TAB_FILTER_CONDIT1..5
					args.add(null);
					args.add(null);
					args.add(null);
					args.add(null);
					args.add(rs.getDo_dp_per_seconds());
					args.add(rs.getDp_tolerance_seconds());
					args.add(dateMode);  // insert value
					args.add(TypeConversionUtil.dateToString(newLastDpDatetime));
					args.add(dateMode);  // update value
					args.add(TypeConversionUtil.dateToString(newLastDpDatetime));
					//args.add(rs.getDp_inst_config_id());
					//dcETLHelper.prepareSql(GlobalSql.UPDATE_DP_CONFIG_LAST_DP_DATETIME);
					//dcETLHelper.setAndExecuteDML(args);
					dcETLHelper.setAndExecuteDML(GlobalSql.UPDATE_DP_CONFIG_LAST_DP_DATETIME, args);
					dcETLHelper.close();
					LogManager.appendToLog("  (3)->End update/insert data purge instance config autoc LAST_DP_DATETIME");
				
					String endDpDate = TypeConversionUtil.dateToString( new Date() );
					LogManager.appendToLog("  Run time [endDpDate:" + endDpDate + "]");
				    
					//=============================
					// <<插入数据清洗log日志表>>
					//=============================
					LogManager.appendToLog("  (4)->Begin insert data purge instance config log");
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(actionFlag);
					args.add(ifaceParameter.getSourceSysKey());
					args.add(rs.getSource_dp_table_owner());
					args.add(sourceDpTableName);
					args.add(sysDbConnMapping.getDb_schema()/*rs.getDest_dp_table_owner()*/);
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(endDpDate);
					args.add(GlobalInfo.SERVICES_RET_SUCCESS);  //process_status:SUCCESS
					args.add("");  // process_message
					//args.add(fullDataPurgeSql);
					args.add(fullDataPurgeSqlBak);
					args.add(logFileContents.toString());  // log_file
					args.add(TypeConversionUtil.dateToString(newLastDpDatetime));
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					if (ifaceParameter.getDpStartDate() != null) {
						args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					} else {
						args.add(TypeConversionUtil.dateToString(rs.getLast_dp_datetime()));
					}
					if (ifaceParameter.getDpEndDate() != null) {
						args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					} else {
						args.add(TypeConversionUtil.dateToString(currentSysDatetime));
					}
					args.add(insertCount);  // process_row_count
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
					
					args.clear();
					
					// 并发控制
					if (concurrentProcessingId != null) {
						new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
					}
					
					LogManager.appendToLog("  (4)->End insert data purge instance config log");
				}
				
				// 提交上述事物
				dcETLHelper.commit();
				
				LogManager.appendToLog("  (5)->commit successfully");
				
			} catch (SQLException e) {
				dcETLHelper.rollback();
				LogManager.appendToLog("  (5)->rollback as SQLException");
				exceptionFlag = true;
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg(" SQLException >> 错误时正在发生的SQL：" + dcETLHelper.getCurrentErrorSql() + 
						GlobalInfo.LINE_SEPARATOR + e.toString());
				e.printStackTrace();
				LogManager.appendToLog("startDataPurgeEngine() => SQLException: " + e.toString(), 
						GlobalInfo.EXCEPTION_MODE);
			} catch (RuntimeException e) {
				dcETLHelper.rollback();
				LogManager.appendToLog("  (5)->rollback as RuntimeException");
				exceptionFlag = true;
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg(" RuntimeException >> 错误时正在发生的SQL：" + dcETLHelper.getCurrentErrorSql() + 
						GlobalInfo.LINE_SEPARATOR + e.toString());
				e.printStackTrace();
				LogManager.appendToLog("startDataPurgeEngine() => RuntimeException: " + e.toString(), 
						GlobalInfo.EXCEPTION_MODE);
			} catch (Exception e) {
				dcETLHelper.rollback();
				LogManager.appendToLog("  (5)->rollback as Exception");
				exceptionFlag = true;
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg(" Exception: " + e.toString());
				e.printStackTrace();
				LogManager.appendToLog("startDataPurgeEngine() => Exception: " + e.toString(), 
						GlobalInfo.EXCEPTION_MODE);
			} finally {
				
				// 插入错误日志
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) && exceptionFlag) {
					args.clear();
					args.add(GeneratorUUID.generateUUID());
					args.add(actionFlag);
					args.add(ifaceParameter.getSourceSysKey());
					args.add(rs.getSource_dp_table_owner());
					args.add(sourceDpTableName);
					args.add(rs.getDest_dp_table_owner());
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(TypeConversionUtil.dateToString( new Date() ));
					args.add(GlobalInfo.SERVICES_RET_ERROR);  //process_status
					args.add(ifaceReturn.getReturnMsg());  // process_message
					args.add("");
					args.add(logFileContents.toString());  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					args.add(null);  // process_row_count
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
					
					// 并发控制
					if (concurrentProcessingId != null) {
						new ConcurrentControlManager(concurrentProcessingId, GlobalInfo.CONCURRENT_PENDING_STS).start();
					}
					
				}
				
				dcETLHelper.close();
			}
			//*************************************************
			
			LogManager.appendToLog("End process data purge: [" + rs.getSource_dp_table_owner() + "." + 
					sourceDpTableName + "](" + ifaceReturn.getReturnStatus() + "," + 
					ifaceReturn.getReturnMsg() + ") --- ");
			LogManager.appendToLog("************************************", GlobalInfo.STATEMENT_MODE);
			
		}  // for QueryDPInstanceConfigsRsEO rs : dpTabList
		
		
		// 关闭连接返回
		try {
			dcETLHelper.closeAll();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ifaceReturn;
	}
	
}
