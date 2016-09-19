package utility;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import component.PostDataPurge;
import utility.LogManager;
import config.GlobalInfo;

public class MqCallDataTransform {
	
	public MqCallDataTransform() throws Exception {
		// TODO Auto-generated constructor stub
		try {
			GlobalInfo.init();
		} catch (IOException e) {
			// TODO: handle exception
			e.printStackTrace();
			LogManager.appendToLog("MqCallDataTransform() => IOException:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		}
	}
	
	//将ssk：pgid：rcid消息转换成ssk.owner.table.pgid.rcid
	public void startDataTransform (String message) throws Exception {
		
		LogManager.appendToLog("Entering MqCallDataTransform.startDataTransform() ---> ");
		
		List<String> sourceSysKeyList = new ArrayList<String>();
		String[] dc2dcetl_infos = new String[]{};
		if (message != null && !"".equals(message)) {
			dc2dcetl_infos = message.split("\\:");
			sourceSysKeyList = SystemHelper.getListenerMessage(dc2dcetl_infos);
		}
		
		// 获取到的后台需要数据清洗的表数
		int sourceSysKeySize = sourceSysKeyList.size();
		
		if (sourceSysKeyList != null && sourceSysKeyList.size() > 0) {
			
			//多线程启动数据清洗处理程序
			MqCallDataPurge.setTotalTabCount(sourceSysKeySize);  // 此处必须设置
			
			for (String sourceSysKeyTab : sourceSysKeyList) {
				new MqCallDataPurge(sourceSysKeyTab,dc2dcetl_infos[1]).start();
			}
		    
			///////////////////////////////////////////////
			//等子线程执行完 主线程在继续（判断条件需要考虑超时的情况）
			///////////////////////////////////////////////
			LogManager.appendToLog("Waiting multi-thread ...... ");
			/*
			while(MqCallDataPurge.tabCount < sourceSysKeySize) {
				TimeUnit.SECONDS.sleep(GlobalInfo.MULTI_THREAD_TEST_PER_SECONDS);  // 15s检测一次
			}
			*/
			synchronized(MqCallDataPurge.lock) {
				MqCallDataPurge.lock.wait();
			}
			///////////////////////////////////////////////
			
			/*
			LogManager.appendToLog("Begin checking task queues process status ---> ");
			if (dc2dcetl_infos != null && dc2dcetl_infos.length > 0) {
				boolean queuesStatusFlag = SystemHelper.checkQueuesStatus(dc2dcetl_infos);
				LogManager.appendToLog(">> checkQueuesStatus(): process [" + message + "]  status：" + Boolean.toString(queuesStatusFlag));
				// 检验是否清洗成功
				// 已成功
				if (queuesStatusFlag == true) {
					// 该批次下当所有表清洗完毕后，修改success 迁移到log
					boolean detailsFlag = SystemHelper.transferQueuesDetails(dc2dcetl_infos[1]);
					boolean queuesFlag = SystemHelper.updateAndTransferQueues(dc2dcetl_infos);
					
					if (detailsFlag == true && queuesFlag == true) {
						LogManager.appendToLog(">>Transfer data to log and update queue status ---> SUCCESS ");
						PostDataPurge.startSendMessage(dc2dcetl_infos[0] + ":" + dc2dcetl_infos[2]);
						LogManager.appendToLog(">>Send MQ[" + dc2dcetl_infos[0] + ":" + dc2dcetl_infos[2] + "] to RmsCal successfully.");
					} else {
						LogManager.appendToLog(">>Transfer data to log and update queue status ---> FAILURE ");
					}
				} else {
					// 未成功
					SystemHelper.rollBackQueuesStatus(dc2dcetl_infos);
				}
			}
			*/
			
			LogManager.appendToLog("End checking task queues process status ---> ");
		} //sourceSysKeyList
		
		LogManager.appendToLog("Leaving MqCallDataTransform.startDataTransform() ---> ");
	}

}
