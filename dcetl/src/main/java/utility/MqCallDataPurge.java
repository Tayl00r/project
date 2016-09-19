package utility;

import factory.DataPurgeETLFactory;
import iface.parameter.IfaceParameters;
import iface.parameter.IfaceReturns;
import config.GlobalInfo;

public class MqCallDataPurge extends Thread{
	
	// 设置一个全局变量tabCount，用来控制多线程是否已结束
	public static int totalTabCount = 0;  // 所有需要清洗的表数，即线程数
	public static int tabCount = 0;
	public static Object lock = new Object();
	
	private String sourceSysKeyTab;
	private String processGroupId;
	
	// setter
	public static void setTotalTabCount(int totalTabCount) {
		MqCallDataPurge.totalTabCount = totalTabCount;
	}
	
	
	public MqCallDataPurge(String sourceSysKeyTab,String processGroupId) {
		// TODO Auto-generated constructor stub
		super();
		this.sourceSysKeyTab = sourceSysKeyTab;
		this.processGroupId = processGroupId;
	}

	@Override
	public void run() {
		
		LogManager.appendToLog("Entering MqCallDataPurge.run() ---> " + sourceSysKeyTab);
		
		try {
			// TODO Auto-generated method stub
			String[] parameter = sourceSysKeyTab.split("\\.");
	
			// 调用数据清洗程序
			IfaceReturns servicesRet = DataPurgeETLFactory.startDataPurgeEngine(new IfaceParameters(parameter[0], 
											  			  				 parameter[1], 
											  			                 parameter[2]),
											  			                 GlobalInfo.DCETL_ENGIN_DO_PURGE);
	
			// 获取返回值
			if (GlobalInfo.SERVICES_RET_SUCCESS.equals(servicesRet.getReturnStatus())) {
				//状态为success时修改process_flag (N ==> Y)
				try {
					//SystemHelper.updateQueuesDetailsProcessStatus(parameter,processGroupId);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				LogManager.appendToLog("  MqstartDataPurgeEngine return: " + servicesRet.getReturnMsg());
				throw new RuntimeException("component.MqCallDataPurge.startDataPurgeEngine("+ sourceSysKeyTab + ") Error: " + 
											servicesRet.getReturnMsg());
			}
		
		} catch(Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("  MqCallDataPurge.run() Exception: " + e.toString());
		} finally {
			//线程结束后，该变量自动+1
			synchronized(lock) {
				this.tabCount++;
				
				// 判断是否唤醒MqCallDataTransform.startDataTransform中的wait
			    if(this.tabCount >= this.totalTabCount) {
			    	lock.notifyAll();
			    }
			}
		}
		
		LogManager.appendToLog("Leaving MqCallDataPurge.run() ---> ");

	}
	
}
