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
import utility.SCPProcessManager;
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
		
		//头结构都是一样的，取出一个即可
		DcProcessTaskQueues dcProcessTaskQueues = dcProcessTaskQueuesList.get(0);
				
		// 检查所有表的处理状态
		int failedCount = 0;
		StringBuffer processErrorMsg = new StringBuffer();
		for(int i=0; i<dcProcessTaskQueuesList.size(); i++) {
			if (dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails() != null) {
				if (dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_flag() != "Y") {
					// 数据清洗过程出错
					failedCount++;
					processErrorMsg.append(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_message());
				} else {
					// 数据清洗过程正常完成
					//==========================
					// 此处增加阳关概念包装的特殊处理逻辑 Added by chao.tang@2016-10-30 for 当发票数据过来后，更新发票对应的PO预测的头order_status状态为APPROVED=>OTHER
					// 此处仅监控invoice_headers/invoice_lines & po_headers_all三个表进行触发
					//==========================
					if (GlobalInfo.SCP_SOURCE_SYS_KEY.equalsIgnoreCase(dcProcessTaskQueues.getSource_sys_key()) && 
							(GlobalInfo.SCP_DCETL_INVOICE_HEADERS.equalsIgnoreCase(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getValue2()) || 
							 GlobalInfo.SCP_DCETL_INVOICE_LINES.equalsIgnoreCase(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getValue2()) || 
							 GlobalInfo.SCP_DCETL_PO_HEADERS_ALL.equalsIgnoreCase(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getValue2()))
						){
						try {
							new SCPProcessManager().process(dcETLHelper, dcProcessTaskQueues.getSource_sys_key());
						} catch (Exception e) {
							dcETLHelper.rollback();
							failedCount++;
							dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().setProcess_flag("W");
							dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().setProcess_message("数据清洗成功完成，但调用SCPProcessManager.process时出错:" + e.toString());
							processErrorMsg.append(dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails().getProcess_message());
						} finally {
							dcETLHelper.close();
						}
					}
					
				} //if--else
			}
		} //for
		
		LogManager.appendToLog(">> Check process status： failedCount=" + failedCount + 
				" processErrorMsg=" + processErrorMsg.toString());
		
		// 检验是否清洗成功
		String status_code = GlobalInfo.DC_QUEUES_SUCCESS_STS;
		if (failedCount > 0) {
			// 未成功，更新状态为ERROR
			if (dcProcessTaskQueues.getRetry_times() < GlobalInfo.MAX_ACHIVE_TASK_RETRY_TIMES) {
				status_code = GlobalInfo.DC_QUEUES_PENDING_STS;  //尝试次数小于3次，状态置为PENDING
			} else {
				status_code = GlobalInfo.DC_QUEUES_ERROR_STS;
				SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 2 , "清洗3次未成功，状态置为ERROR");
			}
		}
		
		dcProcessTaskQueues.setStatus_code(status_code);
		dcProcessTaskQueues.setProcess_message(dcProcessTaskQueues.getRetry_times() + ":" + processErrorMsg.toString());
		
		// 修改任务队列头状态
		try {
			new TaskQueueStatusManager(dcETLHelper, 
					                   dcProcessTaskQueues.getTask_queue_id(),  //task_queue_id
					                   dcProcessTaskQueues.getSource_sys_key(),  //source_sys_key
						               GlobalInfo.DC_QUEUES_TASK_TYPE,  //task_type_code
						               dcProcessTaskQueues.getProcess_group_id(),  //process_group_id
						               dcProcessTaskQueues.getStatus_code(),  //status_code
						               dcProcessTaskQueues.getProcess_message(),  //process_message
						               "N",  //start_process_datetime
						               "Y",  //end_process_datetime
						               "Y"  //retry_times_flag
						               ).updateTaskQueueStatus();
			LogManager.appendToLog("<<Update status[" + status_code + "] successfully>>");
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("PostDataPurge.onCall() => [Update Status: " + status_code + "] Exception:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
			SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 2 , "PostDataPurge.onCall() => [Update Status: " + status_code + "] Exception:" + e.toString());
			muleMsg.setPayload(GlobalInfo.DC_QUEUES_ERROR_STS);
			//关闭数据库连接
			dcETLHelper.rollback(); // 回滚事物
			dcETLHelper.closeAll();
			
			throw new RuntimeException("PostDataPurge.onCall() => [Update Status: " + status_code + "] Exception:" + e.toString());
		}
		
		// 修改队列任务行状态(如果头状态为PENDING，则不更改行的状态)
		if (!GlobalInfo.DC_QUEUES_PENDING_STS.equals(dcProcessTaskQueues.getStatus_code())) {
			List<Object> args = new ArrayList<Object>();
			for(int i=0; i<dcProcessTaskQueuesList.size(); i++) {
				if (dcProcessTaskQueuesList.get(i).getDcProcessTaskQueueDetails() != null) {
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
			} //for
		}
		
		
		//===============================
		// 设置MQ消息，发送至RMSCal 消息队列
		//===============================
		if (failedCount == 0) {
			muleMsg.setPayload(dcProcessTaskQueues.getSource_sys_key() + ":" + dcProcessTaskQueues.getProcess_group_id() + ":" + dcProcessTaskQueues.getActive_node_id());
		} else {
			muleMsg.setPayload(GlobalInfo.DC_QUEUES_ERROR_STS);
		}
		
		try {
			//if (failedCount == 0){
			// 状态为success或者error的情况，则转移到log并且删除主表记录
			if (!GlobalInfo.DC_QUEUES_PENDING_STS.equals(dcProcessTaskQueues.getStatus_code())) {
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
			}
			//}
			
			// 添加node节点监控
			if ("Y".equals(GlobalInfo.ENABLE_ACTIVITY_NODE_FLAG)) {
				SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 1, null);
				LogManager.appendToLog("PostDataPurge.insertActivityNodes() =====>   【 " +GlobalInfo.DC_QUEUES_SUCCESS_STS +" 】");
			}
			
		} catch (Exception e) {
			// 迁移失败，暂时不处理
			e.printStackTrace();
			SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 2, "PostDataPurge.onCall() => [Transfer and delete] Exception:" + e.toString() + GlobalInfo.LINE_SEPARATOR + 
				" 错误发生的sql: " + dcETLHelper.getCurrentErrorSql());
			LogManager.appendToLog("PostDataPurge.insertActivityNodes() =====>   【 " +GlobalInfo.DC_QUEUES_SUCCESS_STS +" 】");
			LogManager.appendToLog("PostDataPurge.onCall() => [Transfer and delete] Exception:" + e.toString() + GlobalInfo.LINE_SEPARATOR + 
					" 错误发生的sql: " + dcETLHelper.getCurrentErrorSql(), GlobalInfo.EXCEPTION_MODE);
		} finally {
			//关闭数据库连接
			dcETLHelper.closeAll();
		}
		

		LogManager.appendToLog("Leaving PostDataPurge.onCall() ---> ");
		
		// 判断是否抛出异常，邮件通知 Added by chao.tang@2016-10-11 14:14:14 Begin
		if (dcProcessTaskQueues != null && dcProcessTaskQueues.getStatus_code() != null && 
				GlobalInfo.DC_QUEUES_ERROR_STS.equals(dcProcessTaskQueues.getStatus_code()) && 
				dcProcessTaskQueues.getRetry_times() >= GlobalInfo.MAX_ACHIVE_TASK_RETRY_TIMES ) {
			SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 2, "Leaving PostDataPurge.onCall(): " + GlobalInfo.LINE_SEPARATOR + 
					GlobalInfo.DB_DCETL_URL + GlobalInfo.LINE_SEPARATOR + dcProcessTaskQueues.getProcess_message());
			throw new RuntimeException("Leaving PostDataPurge.onCall(): " + GlobalInfo.LINE_SEPARATOR + 
					GlobalInfo.DB_DCETL_URL + GlobalInfo.LINE_SEPARATOR + dcProcessTaskQueues.getProcess_message());
		}
		// 判断是否抛出异常，邮件通知 Added by chao.tang@2016-10-11 14:14:14 End

		return muleMsg;
	}

}
