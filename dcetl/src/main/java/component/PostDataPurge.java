package component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

import config.GlobalInfo;
import entity.DcProcessTaskQueues;
import utility.DBHelper;
import utility.LogManager;
import utility.SystemHelper;
import utility.TaskQueueStatusManager;

public class PostDataPurge implements Callable{
	
	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		
		LogManager.appendToLog("Entering PostDataPurge.onCall() ---> ");
		
		MuleMessage muleMsg = eventContext.getMessage();
		
		@SuppressWarnings("unchecked")
		CopyOnWriteArrayList<DcProcessTaskQueues> dcProcessTaskQueuesList = 
				(CopyOnWriteArrayList<DcProcessTaskQueues>) muleMsg.getPayload();
		
		LogManager.appendToLog("dcProcessTaskQueuesList.size(): " + dcProcessTaskQueuesList.size());
		
		DBHelper dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
					GlobalInfo.DB_DCETL_URL, 
					GlobalInfo.DB_DCETL_USER_NAME,
					GlobalInfo.DB_DCETL_PASSWORD, 
					null);
		
		// 检查所有表的处理状态
		int failedCount = 0;
		List<Object> args = new ArrayList<Object>();
		StringBuffer processErrorMsg = new StringBuffer();
		for(int i=0; i<dcProcessTaskQueuesList.size(); i++) {
			if (dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails() != null) {
				if (dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_flag() != "Y") {
					failedCount++;
					processErrorMsg.append(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_message());
				}
				////////////////////////////////////////
				// 更新行状态
				try {
					args.clear();
					args.add(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_flag());
					args.add(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_message());
					args.add(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getType());
					args.add(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_group_id());
					args.add(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getValue1());
					args.add(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getValue2());
					// 更新process_flag N => Y/E
					SystemHelper.updateQueuesDetailsProcessStatus(dcETLHelper, args);
				} catch (Exception e) {
					// 行状态仅为辅助功能，更新失败也不进行处理
				}
				////////////////////////////////////////
			}
		}
		
		LogManager.appendToLog(">> Check process status： failedCount=" + failedCount + 
				" processErrorMsg=" + processErrorMsg.toString());
		
		// 检验是否清洗成功
		String status_code = GlobalInfo.DC_QUEUES_SUCCESS_STS;
		if (failedCount > 0) {
			// 未成功，更新状态为ERROR
			status_code = GlobalInfo.DC_QUEUES_ERROR_STS;
		}
		
		//头结构都是一样的，取出一个即可
		DcProcessTaskQueues dcProcessTaskQueues = dcProcessTaskQueuesList.get(0);
		dcProcessTaskQueues.setStatus_code(status_code);
		dcProcessTaskQueues.setProcess_message(processErrorMsg.toString());
		
		// 修改状态
		try {
			new TaskQueueStatusManager(dcETLHelper, 
					                   dcProcessTaskQueues.getTask_queue_id(),  //task_queue_id
					                   dcProcessTaskQueues.getSource_sys_key(),  //source_sys_key
						               GlobalInfo.DC_QUEUES_TASK_TYPE,  //task_type_code
						               dcProcessTaskQueues.getProcess_group_id(),  //process_group_id
						               dcProcessTaskQueues.getStatus_code(),  //status_code
						               dcProcessTaskQueues.getProcess_message(),  //process_message
						               "N",  //start_process_datetime
						               "Y"  //end_process_datetime
						               ).updateTaskQueueStatus();
			LogManager.appendToLog("<<Update status[" + status_code + "] successfully>>");
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("PostDataPurge.onCall() => [Update Status: " + status_code + "] Exception:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
			
			muleMsg.setPayload(GlobalInfo.DC_QUEUES_ERROR_STS);
			//关闭数据库连接
			dcETLHelper.rollback(); // 回滚事物
			dcETLHelper.closeAll();
			
			throw e;
		}
		
		//===============================
		// 设置MQ消息，发送至RMSCal 消息队列
		//===============================
		if (failedCount == 0) {
			muleMsg.setPayload(dcProcessTaskQueues.getSource_sys_key() + ":" + dcProcessTaskQueues.getSource_ref_doc_id());
		} else {
			muleMsg.setPayload(GlobalInfo.DC_QUEUES_ERROR_STS);
		}
		
		try {
			//if (failedCount == 0){
				// 当所有表成功清洗完毕且状态更新OK后，迁移数据到log并删除原有数据
				boolean detailsFlag = SystemHelper.transferQueuesDetailsAndDelete(dcETLHelper, dcProcessTaskQueues.getProcess_group_id());  //处理明细行表
				
				String[] dc2dcetl_infos = new String[] {dcProcessTaskQueues.getSource_sys_key(),
												dcProcessTaskQueues.getProcess_group_id(),
												dcProcessTaskQueues.getSource_ref_doc_id(),
												dcProcessTaskQueues.getStatus_code()};
				boolean queuesFlag = false;
				if (detailsFlag) {
					queuesFlag = SystemHelper.transferQueuesToLogAndDelete(dcETLHelper, dc2dcetl_infos);  //处理头表
				}
					
				if (detailsFlag == true && queuesFlag == true) {
					LogManager.appendToLog(">>Transfer data to log and delete record ---> SUCCESS ");
					//PostDataPurge.startSendMessage(dc2dcetl_infos[0] + ":" + dc2dcetl_infos[2]);
					LogManager.appendToLog(">>Send MQ[" + dc2dcetl_infos[0] + ":" + dc2dcetl_infos[2] + "] to RmsCal Node.");
				} else {
					LogManager.appendToLog(">>Transfer data to log and delete record[detailsFlag:" + Boolean.toString(detailsFlag) + 
							", queuesFlag:" + Boolean.toString(queuesFlag) + "] ---> FAILURE ");
				}
			//}
		} catch (Exception e) {
			// 迁移失败，暂时不处理
			e.printStackTrace();
			LogManager.appendToLog("PostDataPurge.onCall() => [Transfer and delete] Exception:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
		} finally {
			//关闭数据库连接
			dcETLHelper.closeAll();
		}
			
		LogManager.appendToLog("Leaving PostDataPurge.onCall() ---> ");

		return muleMsg;
	}

}
