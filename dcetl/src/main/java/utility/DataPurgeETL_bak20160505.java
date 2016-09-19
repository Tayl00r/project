/**
 * 
 */
package utility;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import config.GlobalInfo;
import config.GlobalSql;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;
import entity.QueryDPInstanceConfigsRsEO;
import entity.QuerySourceTabColRsEO;
import entity.QueryTabColumnsRsEO;

/**
 * @author Administrator
 *
 */
public class DataPurgeETL_bak20160505 {
	
	private DBHelper dcETLHelper = null;

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
			                              String sourceTabWhereClause) {
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
					" " + tabAliasName + 
					" where (" + sourceTabWhereClause + ")";
		} else {
			selectSql = "select " + sourceTabColumnsList + " from " + 
					(sourceTabName.contains(".")?sourceTabName:sourceTabOwner+"."+sourceTabName) + 
					" " + tabAliasName + 
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
			throws ClassNotFoundException, SQLException {
		
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
		String[] args = new String[]{ sourceSysKey, sourceSysKey, 
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
				rsEO.setDest_dp_table_owner(dcETLHelper.resultSet.getString("DEST_DP_TABLE_OWNER"));
				rsEO.setDest_dp_table_name(dcETLHelper.resultSet.getString("DEST_DP_TABLE_NAME"));
				rsEO.setDo_dp_per_seconds(dcETLHelper.resultSet.getInt("DO_DP_PER_SECONDS"));
				rsEO.setDp_tolerance_seconds(dcETLHelper.resultSet.getInt("DP_TOLERANCE_SECONDS"));
				rsEO.setLast_dp_datetime(dcETLHelper.resultSet.getDate("LAST_DP_DATETIME"));
				rsEO.setDp_inst_config_autoc_id(dcETLHelper.resultSet.getString("DP_INST_CONFIG_AUTOC_ID"));
				rsEO.setSource_sys_key(dcETLHelper.resultSet.getString("SOURCE_SYS_KEY"));
				
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
				validatedFlag =  dcETLHelper.resultSet.getString("VALIDATED_FLAG");
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
		String[] args = new String[]{ sourceSysKey, sourceTabOwner, sourceTabName };
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
				" => 目标returnColumnName: " + returnColumnName, GlobalInfo.STATEMENT_MODE);
		
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
		if (paramDpStartDate == null && paramDpEndDate == null) {
			newLastDpDatetime = currentSysDatetime;  // now()
		} else {
			if (rs_last_dp_datetime != null) {
				if (paramDpStartDate == null && paramDpEndDate != null && 
						paramDpEndDate.compareTo(newLastDpDatetime) >= 0 ) {
					if (paramDpEndDate.compareTo(currentSysDatetime) >= 0) {
						newLastDpDatetime = currentSysDatetime;  // now()
					} else {
						newLastDpDatetime = paramDpEndDate;
					}
				} else if (paramDpStartDate != null && paramDpEndDate == null && 
						paramDpStartDate.compareTo(newLastDpDatetime) <= 0) {
					newLastDpDatetime = currentSysDatetime;  // now()
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
				if (paramDpStartDate == null && paramDpEndDate != null) { 
					if (paramDpEndDate.compareTo(currentSysDatetime) >= 0) {
						newLastDpDatetime = currentSysDatetime;  // now()
					} else {
						newLastDpDatetime = paramDpEndDate;
					}
				}
			} // rs_last_dp_datetime != null
		}
		
		return newLastDpDatetime;
	}
	
	// 数据清理主程序
	public IfaceReturns startDataPurgeEngine(IfaceParameters ifaceParameter,
											 String actionFlag) 
            throws ClassNotFoundException, SQLException, IOException {
		
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
		
		for (QueryDPInstanceConfigsRsEO rs : dpTabList) {
			
			List<Object> args = new ArrayList<Object>();
			String sourceDpTableName = rs.getSource_dp_table_name();  // 清洗的源表
			
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
			
			// 0. 检查当前清洗的表是否存在启用的规则 但 未验证通过
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE) && 
					!checkIsValidDpRules(rs.getSource_sys_key(),
					                 rs.getSource_dp_table_owner(),
					                 sourceDpTableName,
					                 rs.getDest_dp_table_owner(),
					                 rs.getDest_dp_table_name())) {
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
				args.add("");
				args.add("");  // log_file
				args.add(null);
				args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
				args.add(ifaceParameter.getSourceTabOwner());
				args.add(ifaceParameter.getSourceTabName());
				args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
				args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
				
				// 采用异步线程的方式插入
				if (insertDpInstanceLog(dcETLHelper, args)) {
				}
				
				continue;  // 继续下一个表的处理
			}
			
			// 并发控制
			String concurrentProcessingId = null;
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
				concurrentProcessingId = 
						new ConcurrentControlManager(rs.getSource_sys_key(),
								rs.getSource_dp_table_owner(),
								sourceDpTableName,
								rs.getDest_dp_table_owner(),
								rs.getDest_dp_table_name()).isConcurrentDpProcessing();
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
					args.add("");  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					
					// 采用异步线程的方式插入
					if (insertDpInstanceLog(dcETLHelper, args)) {
					}
					continue;  // 继续下一个表的处理
				}
			}
			
			// 1. 获取维护了规则的表列信息（包括规则列表中的列 & 非规则列表维护的列）
			List<QuerySourceTabColRsEO> inRulesTabColList= 
					getDataPurgeTabColumnLists(ifaceParameter.getSourceSysKey(),
						 					   rs.getSource_dp_table_owner(), 
						 					   sourceDpTableName,
						 					   true);
			List<QuerySourceTabColRsEO> notInRulesTabColList= 
					getDataPurgeTabColumnLists(ifaceParameter.getSourceSysKey(),
						 					   rs.getSource_dp_table_owner(), 
						 					   sourceDpTableName,
						 					   false);
			LogManager.appendToLog("  inRulesTabColList.size => " + inRulesTabColList.size());
			LogManager.appendToLog("  notInRulesTabColList.size => " + notInRulesTabColList.size());
			
			// 2. 获取目标表的所有列
			List<QueryTabColumnsRsEO> destTableColList= 
					getDataPurgeTabColumnListsOf(rs.getDest_dp_table_owner(), 
						 					     rs.getDest_dp_table_name());
			LogManager.appendToLog("  destTableColList.size => " + destTableColList.size());
			
			// 3. 获取目标表的主键列
			List<Object> destTabUniqueConstraintColList = getTabUniqueConstraintCol(rs.getDest_dp_table_owner(), 
				     													            rs.getDest_dp_table_name());
			LogManager.appendToLog("  destTabUniqueConstraintColList.size => " + 
				     						destTabUniqueConstraintColList.size());
			
			// 4. 存放 SQL拼接结果
			String selectFromColumnList = "";
			String insertIntoColumnList = "";
			String onDuplicateKeyUptList = ""; 
			String notExistsSql	= "";
			String sourTabOwner	= rs.getSource_dp_table_owner();
			String sourFromTabName = rs.getSource_dp_table_name();
			String sourTabWhereClause = "(1=1)";
			String destTabOwner = rs.getDest_dp_table_owner();
			String destTabName = rs.getDest_dp_table_name();
			String fromConditionSourTabAlias = GlobalInfo.SOUR_TAB_ALIAS_NAME;  // 标的别名，用于替换
			
			if (inRulesTabColList.size() == 0 && notInRulesTabColList.size() == 0) {
				// 没有维护规则的表，但是在 dp_data_purge_inst_configs 表中存在
				List<QueryTabColumnsRsEO> tableColList= 
						getDataPurgeTabColumnListsOf(rs.getSource_dp_table_owner(), 
							 					     sourceDpTableName);
				LogManager.appendToLog("  tableColList.size => " + tableColList.size());
				
				if (tableColList.size() > 0) {
					for (int i=0; i<tableColList.size(); i++) {
						// 判断目标表是否存在该列，进行列匹配插入
						if ( isTabColumnExists(/*rs.getDest_dp_table_owner(),
											   rs.getDest_dp_table_name(),*/
											   destTableColList,
										       tableColList.get(i).getColumn_name()) ) {
							if (selectFromColumnList != null && !"".equals(selectFromColumnList)) {
								selectFromColumnList = selectFromColumnList + "," + 
											tableColList.get(i).getColumn_name();
							} else {
								selectFromColumnList = tableColList.get(i).getColumn_name();
							}
							if (insertIntoColumnList != null && !"".equals(insertIntoColumnList)) {
								insertIntoColumnList = insertIntoColumnList + "," + 
											tableColList.get(i).getColumn_name();
							} else {
								insertIntoColumnList = tableColList.get(i).getColumn_name();
							}
						}
					} // for i 
				} else {
					ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
					ifaceReturn.setReturnMsg(rs.getSource_dp_table_owner() + "." + 
							                 sourceDpTableName + 
							                 " cannot find any valid columns" + "; " + 
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
						args.add("");  // log_file
						args.add(null);
						args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
						args.add(ifaceParameter.getSourceTabOwner());
						args.add(ifaceParameter.getSourceTabName());
						args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
						args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
						
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
				// 根据 规则进行动态拼接列汇总信息
				for (int i=0; i<notInRulesTabColList.size(); i++) {
					sourFromTabName = notInRulesTabColList.get(i).getFrom_condition_tab_list();
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
						if (selectFromColumnList != null && !"".equals(selectFromColumnList)) {
							selectFromColumnList = selectFromColumnList + "," + 
											appendSourTabAlias(destTableColList, 
													sourceDpTableName + "." + notInRulesTabColList.get(i).getSource_tab_column_name(),
													sourceDpTableName,
													fromConditionSourTabAlias);
						} else {
							selectFromColumnList = appendSourTabAlias(destTableColList, 
															sourceDpTableName + "." +  notInRulesTabColList.get(i).getSource_tab_column_name(),
															sourceDpTableName,
															fromConditionSourTabAlias);
						}
						if (insertIntoColumnList != null && !"".equals(insertIntoColumnList)) {
							insertIntoColumnList = insertIntoColumnList + "," + 
									notInRulesTabColList.get(i).getDest_tab_column_name();
						} else {
							insertIntoColumnList = notInRulesTabColList.get(i).getDest_tab_column_name();
						}
					}
				} // for i 
				//////////////////////////////////////////////////////////////
				for (int j=0; j<inRulesTabColList.size(); j++) {
					sourFromTabName = inRulesTabColList.get(j).getFrom_condition_tab_list();
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
						if (selectFromColumnList != null && !"".equals(selectFromColumnList)) {
							selectFromColumnList = selectFromColumnList + "," + 
										appendSourTabAlias(destTableColList, 
												inRulesTabColList.get(j).getDp_return_column_val(),
												sourceDpTableName,
												fromConditionSourTabAlias);
						} else {
							selectFromColumnList = appendSourTabAlias(destTableColList, 
															inRulesTabColList.get(j).getDp_return_column_val(),
															sourceDpTableName,
															fromConditionSourTabAlias);
						}
						if (insertIntoColumnList != null && !"".equals(insertIntoColumnList)) {
							insertIntoColumnList = insertIntoColumnList + "," + 
									inRulesTabColList.get(j).getDest_tab_column_name();
						} else {
							insertIntoColumnList = inRulesTabColList.get(j).getDest_tab_column_name();
						}
						
						// 存在规则的话，则更新规则的列值
						if (!"".equals(onDuplicateKeyUptList) && onDuplicateKeyUptList != null) {
							onDuplicateKeyUptList = onDuplicateKeyUptList + ", " + 
									inRulesTabColList.get(j).getDest_tab_column_name() + 
									"=" + 
									appendSourTabAlias(destTableColList, 
											inRulesTabColList.get(j).getDp_return_column_val(),
											sourceDpTableName,
											fromConditionSourTabAlias);
						} else {
							onDuplicateKeyUptList = inRulesTabColList.get(j).getDest_tab_column_name() + 
									"=" + 
									appendSourTabAlias(destTableColList, 
											inRulesTabColList.get(j).getDp_return_column_val(),
											sourceDpTableName,
											fromConditionSourTabAlias);
						}
						
					}
				} // for j 
			}
			
			
			//*************************************************
			// 5. 进行sql拼接
			LogManager.appendToLog("  insertIntoColumnList: " + insertIntoColumnList, GlobalInfo.STATEMENT_MODE);
			LogManager.appendToLog("  selectFromColumnList: " + selectFromColumnList, GlobalInfo.STATEMENT_MODE);
			if (insertIntoColumnList.length() == 0 || selectFromColumnList.length() == 0) {
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
					args.add("");  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					
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
			
			// 构造各个部分的sql
			String selectSql = generateSourceQuerySql(ifaceParameter, 
					sourTabOwner, 
					sourFromTabName, 
					selectFromColumnList,   // 多表连接，需要进行制定列名前缀
					sourTabWhereClause);
			String insertIntoSql = generateDestInsertSql(ifaceParameter, 
					destTabOwner, 
					destTabName, 
					insertIntoColumnList);
			LogManager.appendToLog("  selectSql: " + selectSql, GlobalInfo.STATEMENT_MODE);
			LogManager.appendToLog("  insertIntoSql: " + insertIntoSql, GlobalInfo.STATEMENT_MODE);
			
			if (inRulesTabColList.size() == 0) {
				// 不存在规则的话，则用not exists 过滤数据，不进行更新
				notExistsSql = "1=1";
				for (int i=0; i<destTabUniqueConstraintColList.size(); i++) {
					notExistsSql = notExistsSql + " and " + 
							GlobalInfo.DEST_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString() + 
							"=" + 
							GlobalInfo.SOUR_TAB_ALIAS_NAME + "." + destTabUniqueConstraintColList.get(i).toString();
				}
				notExistsSql = "not exists (" + 
				               "   select 1 " + 
						       "     from " + destTabOwner + "." + destTabName + " " + GlobalInfo.DEST_TAB_ALIAS_NAME + 
						       "	where " + notExistsSql + 
				               "             )";
			}
			
			// 最终的组装
			String fullDataPurgeSql = insertIntoSql + 
					System.getProperty("line.separator") + 
					selectSql;
			
			boolean startDateFilterConditFlag = false;
			boolean endDateFilterConditFlag = false;
			boolean dp_tab_filter_condit3_flag = false;
			boolean dp_tab_filter_condit4_flag = false;
			boolean dp_tab_filter_condit5_flag = false;
			int filterConditCount = 0;
			
			// 第一个字段表示dpStartDate
			if (rs.getSource_dp_tab_filter_condit1() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit1())) {
				fullDataPurgeSql = fullDataPurgeSql + " and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit1(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")";
				startDateFilterConditFlag = true;
				filterConditCount = filterConditCount + 1;
			}
			// 第二个字段表示dpEndDate
			if (rs.getSource_dp_tab_filter_condit2() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit2())) {
				fullDataPurgeSql = fullDataPurgeSql + " and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit2(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")";
				endDateFilterConditFlag = true;
				filterConditCount = filterConditCount + 1;
			}
			// source_sys_key
			if (rs.getSource_dp_tab_filter_condit3() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit3())) {
				fullDataPurgeSql = fullDataPurgeSql + " and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit3(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")";
				dp_tab_filter_condit3_flag = true;
				filterConditCount = filterConditCount + 1;
			}
			// 后面的字段4,5暂不替换
			if (rs.getSource_dp_tab_filter_condit4() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit4())) {
				fullDataPurgeSql = fullDataPurgeSql + " and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit4(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")";
				dp_tab_filter_condit4_flag = true;
				filterConditCount = filterConditCount + 1;
			}
			if (rs.getSource_dp_tab_filter_condit5() != null && 
					!"".equals(rs.getSource_dp_tab_filter_condit5())) {
				fullDataPurgeSql = fullDataPurgeSql + " and (" + 
						appendSourTabAlias(destTableColList, 
								rs.getSource_dp_tab_filter_condit5(),
								sourceDpTableName,
								fromConditionSourTabAlias) + ")";
				dp_tab_filter_condit5_flag = true;
				filterConditCount = filterConditCount + 1;
			}
			
			if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_VALIDATION_RULES)) {
				fullDataPurgeSql = fullDataPurgeSql + " and (1=2)";
			}
			
			// on duplicate key update 判断
			if (inRulesTabColList.size() > 0) {
				if (onDuplicateKeyUptList != null && !"".equals(onDuplicateKeyUptList)) {
					fullDataPurgeSql = fullDataPurgeSql + System.getProperty("line.separator") + 
							" on duplicate key update " + onDuplicateKeyUptList;
				}
			} else {
				if (notExistsSql != null && !"".equals(notExistsSql)) {
					fullDataPurgeSql = fullDataPurgeSql + System.getProperty("line.separator") + 
							" and (" + notExistsSql + ")";
				}
			}
			
			LogManager.appendToLog("  *** fullDataPurgeSql: " + fullDataPurgeSql);
			
			// 6. 执行 数据清洗脚本 & 更新配置表的上一次数据清洗时间 + 插入数据清洗log日志表
			args.clear();
			if (startDateFilterConditFlag) {
				args.add(ifaceParameter.getDpStartDate());
				args.add(rs.getLast_dp_datetime());
				args.add(rs.getDp_tolerance_seconds());
			}
			if (endDateFilterConditFlag) {
				args.add(ifaceParameter.getDpEndDate());
			}
			// set source_sys_key
			if (dp_tab_filter_condit3_flag) {
				args.add(ifaceParameter.getSourceSysKey());
			}
			
			Date currentSysDatetime = new java.sql.Date(new Date().getTime());  // get current sysdate
			boolean exceptionFlag = false;
			
			try {
				// <<execute query>>
				LogManager.appendToLog("  (2)->begin execute fullDataPurgeSql ", GlobalInfo.DEBUG_MODE);
				//dcETLHelper.prepareSql(fullDataPurgeSql);
				//dcETLHelper.setAndExecuteDML(args);
				dcETLHelper.setAndExecuteDML(fullDataPurgeSql, args);
				LogManager.appendToLog("  (2)->end execute fullDataPurgeSql ", GlobalInfo.DEBUG_MODE);
				
				
				if (actionFlag.equals(GlobalInfo.DCETL_ENGIN_DO_PURGE)) {
					//=============================
					// <<更新配置表的上一次数据清洗时间>>
					//=============================
					LogManager.appendToLog("  (3)->Begin update/insert data purge instance config autoc LAST_DP_DATETIME");
					
					// 新的上一次数据清洗值，根据条件进行设置
					Date newLastDpDatetime = getNewLastDpDatetime(rs.getLast_dp_datetime(),
																  currentSysDatetime,
																  ifaceParameter.getDpStartDate(),
																  ifaceParameter.getDpEndDate());
					String dateMode = GlobalInfo.OTHERS_DATE_VAL_MODE;
					LogManager.appendToLog("  newLastDpDatetime: [" + dateMode + "]" + 
							TypeConversionUtil.dateToString(newLastDpDatetime));
					
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
					args.add(rs.getDest_dp_table_owner());
					args.add(rs.getDest_dp_table_name());
					args.add(startDpDate);
					args.add(endDpDate);
					args.add(GlobalInfo.SERVICES_RET_SUCCESS);  //process_status:SUCCESS
					args.add("");  // process_message
					args.add(fullDataPurgeSql);
					args.add("");  // log_file
					args.add(TypeConversionUtil.dateToString(newLastDpDatetime));
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					
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
				ifaceReturn.setReturnMsg(" 错误时正在发生的SQL：" + dcETLHelper.getCurrentErrorSql() + 
						System.getProperty("line.separator") + 
						e.toString());
				e.printStackTrace();
				LogManager.appendToLog("startDataPurgeEngine() => SQLException: " + e.toString(), 
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
					args.add("");  // log_file
					args.add(null);
					args.add(ifaceParameter.getSourceSysKey());  // dp_parameter1..5
					args.add(ifaceParameter.getSourceTabOwner());
					args.add(ifaceParameter.getSourceTabName());
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpStartDate()));
					args.add(TypeConversionUtil.dateToString(ifaceParameter.getDpEndDate()));
					
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
