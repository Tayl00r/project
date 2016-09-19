package factory;

import java.io.IOException;
import java.sql.SQLException;

import utility.DataPurgeETL;
import config.GlobalInfo;
import iface.impl.DataPurgeConfiguration;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;

public class DataPurgeETLFactory {

	// 数据清理工厂主程序
	public static IfaceReturns startDataPurgeEngine(IfaceParameters ifaceParameter,
											        String actionFlag) {
		
		IfaceReturns servicesRet = new IfaceReturns();
		
		// 校验参数
		if (ifaceParameter.getSourceSysKey() == null || "".equals(ifaceParameter.getSourceSysKey())) {
			servicesRet.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			servicesRet.setReturnMsg("接口参数 sourceSysKey 不能为空");
			
			return servicesRet;
		}
		
		try {
			// 初始化全局变量信息
			GlobalInfo.init();
			
			// 检查基本的配置是否已设置
			servicesRet = DataPurgeConfiguration.checkDpConfig(ifaceParameter, actionFlag);
			
			// 启动数据清理引擎
			if (servicesRet.getReturnStatus().equals(GlobalInfo.SERVICES_RET_SUCCESS)) {
				servicesRet = new DataPurgeETL().startDataPurgeEngine(ifaceParameter, actionFlag);
			}
		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			servicesRet.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			servicesRet.setReturnMsg(e.toString());
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			servicesRet.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			servicesRet.setReturnMsg(e.toString());
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			servicesRet.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			servicesRet.setReturnMsg(e.toString());
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			servicesRet.setReturnStatus(GlobalInfo.SERVICES_RET_ERROR);
			servicesRet.setReturnMsg(e.toString());
			e.printStackTrace();
		}
		
		return servicesRet;
	}
	
}
