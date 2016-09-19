package iface.impl;

import java.sql.SQLException;

import utility.DBHelper;
import utility.LogManager;
import config.GlobalInfo;
import config.GlobalSql;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;

public class DataPurgeConfiguration {
	
	// 校验数据清洗相关的设置
	public static IfaceReturns checkDpConfig(IfaceParameters ifaceParameter,
			 							     String actionFlag)
			 throws ClassNotFoundException, SQLException {
		
		LogManager.appendToLog("Entering checkDpConfig() ---> ");
		
		IfaceReturns ifaceReturn = new IfaceReturns();
			
		int totalTabColCount = 0;  // 返回列数合计
			
		DBHelper dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
						    				GlobalInfo.DB_DCETL_URL, 
						    				GlobalInfo.DB_DCETL_USER_NAME,
						    				GlobalInfo.DB_DCETL_PASSWORD, 
						    				null);
		String[] args = new String[]{ ifaceParameter.getSourceTabName(),
				                      ifaceParameter.getSourceSysKey(), 
									  GlobalInfo.LOOKUP_TYPE_DCETL_SOUR_SYS_KEY };
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.CHK_DCETL_SOUR_SYS_KEY_LOOKUP, (Object[])args);
			if (dcETLHelper.resultSet.next()) {
				totalTabColCount =  dcETLHelper.resultSet.getInt("TOTAL_ROW_COUNT");
			}
			
			if (totalTabColCount == 0) {
				ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
				ifaceReturn.setReturnMsg("数据清洗规则校验失败: 请维护快码[" + GlobalInfo.LOOKUP_TYPE_DCETL_SOUR_SYS_KEY + 
						"]对应的来源[" + ifaceParameter.getSourceSysKey() + 
						"]中的快码值[" + ifaceParameter.getSourceTabName() + "].");
			}
			
		} catch (Exception e) {
			ifaceReturn.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			ifaceReturn.setReturnMsg(e.toString());
			e.printStackTrace();
			LogManager.appendToLog("checkDpConfig() => Exception:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.closeAll();
		}
		
		LogManager.appendToLog("Leaving checkDpConfig() ---> totalTabColCount:" + totalTabColCount);
		
		return ifaceReturn;
	}

}
