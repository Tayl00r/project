package component;

import factory.DataPurgeETLFactory;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;

import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

import config.GlobalInfo;
import entity.DcProcessTaskQueueDetails;
import entity.DcProcessTaskQueues;
import utility.LogManager;
import utility.SystemHelper;

public class CallDataPurgeWebservices implements Callable {

	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		// TODO Auto-generated method stub
		
		LogManager.appendToLog("Entering CallDataPurgeWebservices.onCall() ---> ");
		
		MuleMessage muleMsg = eventContext.getMessage();
		
		// 单个payload 消息
		
		LogManager.appendToLog("*************************************" + muleMsg.getPayload().toString());
		
		DcProcessTaskQueues dcProcessTaskQueues = (DcProcessTaskQueues)muleMsg.getPayload();
		
		if (dcProcessTaskQueues != null) {
			String sourceSysKeyTab = dcProcessTaskQueues.getSource_sys_key() + "." + 
					dcProcessTaskQueues.getDcProcessTaskQueueDetails().getValue1() + "." + 
					dcProcessTaskQueues.getDcProcessTaskQueueDetails().getValue2();
					
			LogManager.appendToLog("  >>muleMsg.getPayload() => sourceSysKeyTab: " + sourceSysKeyTab);
		
			if (sourceSysKeyTab != null && !"".equals(sourceSysKeyTab)) {
				String[] parameter = new String[]{ dcProcessTaskQueues.getSource_sys_key(), 
												   dcProcessTaskQueues.getDcProcessTaskQueueDetails().getValue1(),  //Schema
												   dcProcessTaskQueues.getDcProcessTaskQueueDetails().getValue2(),  //表名
												   dcProcessTaskQueues.getDcProcessTaskQueueDetails().getValue3(),  //开始清洗时间
												   dcProcessTaskQueues.getDcProcessTaskQueueDetails().getValue4()   //结束清洗时间
												   };
				
				// 调用数据清洗程序		    
				IfaceReturns servicesRet = 
						DataPurgeETLFactory.startDataPurgeEngine(new IfaceParameters(parameter[0], parameter[1], parameter[2], parameter[3], parameter[4], "N"), 
								GlobalInfo.DCETL_ENGIN_DO_PURGE);
				
				LogManager.appendToLog("  >>startDataPurgeEngine return: " + servicesRet.getReturnMsg() + GlobalInfo.LINE_SEPARATOR + 
						servicesRet.getReturnMsg());
				// 获取返回值
				DcProcessTaskQueueDetails detail = dcProcessTaskQueues.getDcProcessTaskQueueDetails();
				if (GlobalInfo.SERVICES_RET_SUCCESS.equals(servicesRet.getReturnStatus())) {
					detail.setProcess_flag("Y");
					detail.setProcess_message("");
					dcProcessTaskQueues.setDcProcessTaskQueueDetails(detail);
				} else if (GlobalInfo.SERVICES_RET_WARN.equals(servicesRet.getReturnStatus())) {
					detail.setProcess_flag("W");  // WARNING
					detail.setProcess_message("Call DataPurgeETLFactory.startDataPurgeEngine() return Warn: " + servicesRet.getReturnMsg());
					dcProcessTaskQueues.setDcProcessTaskQueueDetails(detail);
				} else {
					detail.setProcess_flag("E");  // Error
					detail.setProcess_message("Call DataPurgeETLFactory.startDataPurgeEngine() return Error: " + servicesRet.getReturnMsg());
					dcProcessTaskQueues.setDcProcessTaskQueueDetails(detail);
				}
				
				muleMsg.setPayload(dcProcessTaskQueues);
				
			}
		}  // dcProcessTaskQueues != null
		
		LogManager.appendToLog("Leaving CallDataPurgeWebservices.onCall() ---> ");
		
		return muleMsg;
	}

}
