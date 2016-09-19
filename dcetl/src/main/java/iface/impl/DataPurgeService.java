/**
 * 
 */
package iface.impl;

import utility.LogManager;
import iface.IDataPurgeService;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;
import config.GlobalInfo;
import factory.DataPurgeETLFactory;

/**
 * @author Administrator
 *
 */
public class DataPurgeService implements IDataPurgeService {

	/* (non-Javadoc)
	 * @see iface.IDataPurgeService#validateDpRulesService(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String validateDpRulesService(String sourceSysKey,
			String sourceTabOwner, String sourceTabName) {
		// TODO Auto-generated method stub
		String returnStatus = GlobalInfo.SERVICES_RET_SUCCESS;
		String returnMsg = "";
		
		// 参数为空时的处理
		if (sourceSysKey == null || "".equals(sourceSysKey)) {
			sourceSysKey = "";
		}
		if (sourceTabOwner == null || "".equals(sourceTabOwner)) {
			sourceTabOwner = null;
		}
		if (sourceTabName == null || "".equals(sourceTabName)) {
			sourceTabName = null;
		}
		
		LogManager.appendToLog("param => sourceSysKey:" + sourceSysKey);
		LogManager.appendToLog("param => sourceTabOwner:" + sourceTabOwner);
		LogManager.appendToLog("param => sourceTabName:" + sourceTabName);
		LogManager.appendToLog("param => dpStartDate:");
		LogManager.appendToLog("param => dpEndDate:");
		LogManager.appendToLog("param => actionFlag:" + GlobalInfo.DCETL_ENGIN_VALIDATION_RULES);
		
		// set parameter
		IfaceParameters ifaceParameter = 
				new IfaceParameters(sourceSysKey, sourceTabOwner, sourceTabName);
	    
		// 以 验证规则的方式 启动数据清洗
		IfaceReturns servicesRet = 
				DataPurgeETLFactory.startDataPurgeEngine(ifaceParameter, GlobalInfo.DCETL_ENGIN_VALIDATION_RULES);
		
		// 获取返回值
		returnStatus = servicesRet.getReturnStatus();
		returnMsg = servicesRet.getReturnMsg();
		
		return returnStatus + GlobalInfo.SERVICES_RET_SEPARATOR + returnMsg;
	}

	/* (non-Javadoc)
	 * @see iface.IDataPurgeService#startDpService(java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public String startDpService(String sourceSysKey, String sourceTabOwner,
			String sourceTabName, String dpStartDate, String dpEndDate,
			String initialDataImpFlag) {
		// TODO Auto-generated method stub
		String returnStatus = GlobalInfo.SERVICES_RET_SUCCESS;
		String returnMsg = "";
		
		// 参数为空时的处理
		if (sourceSysKey == null || "".equals(sourceSysKey)) {
			sourceSysKey = "";
		}
		if (sourceTabOwner == null || "".equals(sourceTabOwner)) {
			sourceTabOwner = null;
		}
		if (sourceTabName == null || "".equals(sourceTabName)) {
			sourceTabName = null;
		}
		
		LogManager.appendToLog("param => sourceSysKey:" + sourceSysKey);
		LogManager.appendToLog("param => sourceTabOwner:" + sourceTabOwner);
		LogManager.appendToLog("param => sourceTabName:" + sourceTabName);
		LogManager.appendToLog("param => dpStartDate:" + dpStartDate);
		LogManager.appendToLog("param => dpEndDate:" + dpEndDate);
		LogManager.appendToLog("param => initialDataImpFlag:" + initialDataImpFlag);
		LogManager.appendToLog("param => actionFlag:" + GlobalInfo.DCETL_ENGIN_DO_PURGE);
				
		// set parameter
		IfaceParameters ifaceParameter = 
				new IfaceParameters(sourceSysKey, sourceTabOwner, sourceTabName, dpStartDate, dpEndDate, initialDataImpFlag);
			    
		// 以 验证规则的方式 启动数据清洗
		IfaceReturns servicesRet = 
				DataPurgeETLFactory.startDataPurgeEngine(ifaceParameter, GlobalInfo.DCETL_ENGIN_DO_PURGE);
			
		// 获取返回值
		returnStatus = servicesRet.getReturnStatus();
		returnMsg = servicesRet.getReturnMsg();
		
		return returnStatus + GlobalInfo.SERVICES_RET_SEPARATOR + returnMsg;
	}

}
