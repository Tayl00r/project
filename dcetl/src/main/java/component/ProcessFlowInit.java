package component;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

import utility.DBHelper;
import utility.DbConnMappingUtil;
import utility.LogManager;
import utility.SystemHelper;
import utility.TaskQueueStatusManager;
import utility.TypeConversionUtil;
import config.GlobalInfo;
import config.GlobalSql;
import entity.DcProcessTaskQueueDetails;
import entity.DcProcessTaskQueues;
import entity.DcSysDbConnMapping;

public class ProcessFlowInit implements Callable {
    
	private DBHelper dcETLHelper;
	private DcProcessTaskQueues dcProcessTaskQueues;
	
	// 构造函数
	public ProcessFlowInit() throws Exception {
		try {
			GlobalInfo.init();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LogManager.appendToLog("ProcessFlowInit() => IOException:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		}
	}
	
	@SuppressWarnings({ "unchecked", "unused" })
	public Object onCall(MuleEventContext eventContext) throws Exception {
		// TODO Auto-generated method stub
		
		LogManager.appendToLog("============================================================================", GlobalInfo.STATEMENT_MODE);
		LogManager.appendToLog("Entering ProcessFlowInit.onCall() ---> ");
		
		MuleMessage muleMsg = eventContext.getMessage();
		
		// 获取MQ的消息，默认从表启动的话，payload是获取不到信息的
		Map<String,String> messageMap = new HashMap<String,String>();
		if (muleMsg.getPayload() != null && !"".equals(muleMsg.getPayload())) {
			messageMap = (Map<String, String>)muleMsg.getPayload();
		}
		String mqMessage = (messageMap != null?messageMap.get("all2dcMessage"):"");
		LogManager.appendToLog("============================================================================");
		LogManager.appendToLog(">>Message Listener got the all2dcMessage: " + mqMessage + " @" + 
				TypeConversionUtil.dateToString(new Date()) + "<<");
		LogManager.appendToLog("============================================================================");
		
		dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		                		   GlobalInfo.DB_DCETL_URL, 
		                		   GlobalInfo.DB_DCETL_USER_NAME,
		                		   GlobalInfo.DB_DCETL_PASSWORD, 
		                		   null);
		
		List<DcProcessTaskQueues> dcProcessTaskQueuesList = new ArrayList<DcProcessTaskQueues>();
		
		// Get Process Task and Lock
		if (getProcessTaskAndLock(mqMessage)) {
			
			LogManager.appendToLog("Get the list of data purge tables", GlobalInfo.STATEMENT_MODE);
			// 获取需要清洗的表
			String[] args = new String[]{ GlobalInfo.LOOKUP_TYPE_DCETL_SOUR_SYS_KEY,
									dcProcessTaskQueues.getTask_queue_id(),
									dcProcessTaskQueues.getSource_sys_key(),
									dcProcessTaskQueues.getProcess_group_id(),
									dcProcessTaskQueues.getSource_ref_doc_id(),
									GlobalInfo.DC_QUEUES_RUNNING_STS,
									GlobalInfo.DC_QUEUES_TASK_TYPE,
									GlobalInfo.DC_QUEUE_DETAILS_TYPE };
			try {
				// execute query
				dcETLHelper.executeQuery(GlobalSql.QUERY_SOURCE_SYS_KEY_TAB_LIST_NEW, (Object[])args);
				while (dcETLHelper.resultSet.next()) {
					DcProcessTaskQueues headers = new DcProcessTaskQueues(dcProcessTaskQueues);
					DcProcessTaskQueueDetails details = new DcProcessTaskQueueDetails();
					
					//设置行属性
					details.setProcess_group_id(dcETLHelper.resultSet.getString("process_group_id"));
					details.setType(dcETLHelper.resultSet.getString("type"));
					details.setValue1(dcETLHelper.resultSet.getString("value1"));
					details.setValue2(dcETLHelper.resultSet.getString("value2"));
					details.setValue3(dcETLHelper.resultSet.getString("value3"));
					details.setValue4(dcETLHelper.resultSet.getString("value4"));
					details.setValue5(dcETLHelper.resultSet.getString("value5"));
					details.setProcess_flag("N");
					details.setProcess_message(null);
					
					//添加到队列中
					headers.setDcProcessTaskQueueDetails(details);
					dcProcessTaskQueuesList.add(headers);
				}
				
				// 判断是否有需要清洗的表
				if (dcProcessTaskQueuesList.size() == 0) {
					dcProcessTaskQueues.setStatus_code(GlobalInfo.DC_QUEUES_ERROR_STS);
					dcProcessTaskQueues.setProcess_message("ProcessFlowInit.onCall()-->未找到满足条件需要清洗的表，可能原因: 请维护快码[" + GlobalInfo.LOOKUP_TYPE_DCETL_SOUR_SYS_KEY + 
						"]对应的来源[" + dcProcessTaskQueues.getSource_sys_key() + "]中的快码值[表清单] 或 表dc_process_task_queue_details未包含任何记录，请检查. " + 
						"***当前执行检查的SQL: " + dcETLHelper.getCurrentErrorSql());
				} else {
					//========================================
					//初始化分库分表数据库连接信息实体
					try {
						DcSysDbConnMapping connMapping = DbConnMappingUtil.initDbConnMapping(dcETLHelper, dcProcessTaskQueues.getSource_sys_key());
					} catch (Exception e) {
						//e.printStackTrace();
						LogManager.appendToLog("ProcessFlowInit.onCall() => InitSysDbConnMapping ---> Exception:" + e.toString(),
								GlobalInfo.EXCEPTION_MODE);
						//
						dcProcessTaskQueues.setStatus_code(GlobalInfo.DC_QUEUES_ERROR_STS);
						dcProcessTaskQueues.setProcess_message("ProcessFlowInit.onCall()-->未找到source_sys_key=" + dcProcessTaskQueues.getSource_sys_key() + 
								" 目标数据库的连接配置信息，请检查表dc_sys_db_conn_mapping的配置. " + 
								"***当前执行检查的SQL: " + dcETLHelper.getCurrentErrorSql());
					}
					//========================================
				}
				
			} catch (Exception e) {
				dcProcessTaskQueues.setStatus_code(GlobalInfo.DC_QUEUES_ERROR_STS);
				dcProcessTaskQueues.setProcess_message("ProcessFlowInit.onCall()-->添加待清洗表清单至队列时出错: " + e.toString() + ".");
				dcProcessTaskQueuesList.clear();  // 清除已添加队列内容
				e.printStackTrace();
				LogManager.appendToLog("ProcessFlowInit.onCall() => Exception:" + e.toString(),
						GlobalInfo.EXCEPTION_MODE);
			} finally {
				// 由于表已锁住，更新处理状态为error
				if (dcProcessTaskQueues != null && dcProcessTaskQueues.getStatus_code() != null && 
						GlobalInfo.DC_QUEUES_ERROR_STS.equals(dcProcessTaskQueues.getStatus_code())) {
					String currentStatusCode = dcProcessTaskQueues.getStatus_code();
					if (dcProcessTaskQueues.getRetry_times() < GlobalInfo.MAX_ACHIVE_TASK_RETRY_TIMES) {
						currentStatusCode = GlobalInfo.DC_QUEUES_PENDING_STS;  //尝试次数小于3次，状态置为PENDING
					}
					try {
						new TaskQueueStatusManager(dcETLHelper, 
				                   dcProcessTaskQueues.getTask_queue_id(),  //task_queue_id
				                   dcProcessTaskQueues.getSource_sys_key(),  //source_sys_key
				                   GlobalInfo.DC_QUEUES_TASK_TYPE,  //task_type_code
				                   dcProcessTaskQueues.getProcess_group_id(),  //process_group_id
				                   currentStatusCode,  //status_code
				                   dcProcessTaskQueues.getRetry_times() + ":" + dcProcessTaskQueues.getProcess_message(),  //process_message
				                   "N",  //start_process_datetime
								   "Y",  //end_process_datetime
								   "Y"  //retry_times_flag
				                   ).updateTaskQueueStatus();
					} catch (Exception e) {
						e.printStackTrace();
					}
					
					// 添加node节点监控 (避免queues没有对应的details，造成监控失败)
					if ("Y".equals(GlobalInfo.ENABLE_ACTIVITY_NODE_FLAG)) {
						SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 1 ,null);
						LogManager.appendToLog("ProcessFlowInit.updateActivityNodes() =====>   【 " + GlobalInfo.DC_QUEUES_SUCCESS_STS +" 】");
					}
					
				}
				//关闭数据库连接
				release();
			}
		    
			muleMsg.setPayload(dcProcessTaskQueuesList);
			
		} else {
			// 记录已被处理 或 记录Lock 失败 ( lock 失败情况很多 )
			muleMsg.setPayload(dcProcessTaskQueuesList);
		}
		
		// 关闭数据库连接
	    release();
		
		LogManager.appendToLog("  sourceSysKeyList.size() = " + dcProcessTaskQueuesList.size() );
		LogManager.appendToLog("Leaving ProcessFlowInit.onCall() ---> ");
		
		// 判断是否抛出异常，邮件通知 Added by chao.tang@2016-10-11 14:14:14 Begin
		if (dcProcessTaskQueues != null && dcProcessTaskQueues.getStatus_code() != null && 
				GlobalInfo.DC_QUEUES_ERROR_STS.equals(dcProcessTaskQueues.getStatus_code()) && 
				dcProcessTaskQueues.getRetry_times() >= GlobalInfo.MAX_ACHIVE_TASK_RETRY_TIMES ) {
			SystemHelper.updateNodes(dcProcessTaskQueues.getActive_node_id(), 2 , GlobalInfo.DB_DCETL_URL + GlobalInfo.LINE_SEPARATOR + dcProcessTaskQueues.getProcess_message());
			LogManager.appendToLog("ProcessFlowInit.updateActivityNodes() =====>   【 " + GlobalInfo.DB_DCETL_URL + GlobalInfo.LINE_SEPARATOR + dcProcessTaskQueues.getProcess_message() +" 】");
			throw new RuntimeException(GlobalInfo.DB_DCETL_URL + GlobalInfo.LINE_SEPARATOR + dcProcessTaskQueues.getProcess_message());
		}
		
		return muleMsg;
	}
	
	// 获取处理任务并锁住任务
	private boolean getProcessTaskAndLock(String mqMessage) {
		
		LogManager.appendToLog("Entering getProcessTaskAndLock("+ mqMessage + ") ---> @" + 
				TypeConversionUtil.dateToString(new Date()));
		
		boolean flag = false;  //返回值
		
		String[] args = null;
		String parentNodeId = null;
		if (mqMessage != null && !"".equals(mqMessage)) {
			// 监听到消息，走MQ的逻辑
			String[] argsSource = mqMessage.split("\\:");
			args = TypeConversionUtil.getRemove(argsSource);
			parentNodeId = mqMessage.split("\\:")[3];
		} else {
			//主动扫表的逻辑
			args = new String[]{null, null, null};
		}
		
		/////////////////////////////////////
		// Get data
		/////////////////////////////////////
		dcProcessTaskQueues = new DcProcessTaskQueues();
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.QUERY_PROCESS_TASK_QUEUE_LIMIT1, (Object[])args);
			while (dcETLHelper.resultSet.next()) {
				dcProcessTaskQueues.setTask_queue_id(dcETLHelper.resultSet.getString("task_queue_id"));
				dcProcessTaskQueues.setTask_type_code(dcETLHelper.resultSet.getString("task_type_code"));
				dcProcessTaskQueues.setSource_sys_key(dcETLHelper.resultSet.getString("source_sys_key"));
				dcProcessTaskQueues.setProcess_group_id(dcETLHelper.resultSet.getString("process_group_id"));
				dcProcessTaskQueues.setStatus_code(dcETLHelper.resultSet.getString("status_code"));
				dcProcessTaskQueues.setProcess_message(null);
				dcProcessTaskQueues.setSource_ref_doc_id(dcETLHelper.resultSet.getString("source_ref_doc_id"));
				dcProcessTaskQueues.setRetry_times(dcETLHelper.resultSet.getInt("retry_times"));
			}
			
		} catch (Exception e) {
			try {
				dcETLHelper.rollback();
			} catch (SQLException sqle) {	
			}
			e.printStackTrace();
			LogManager.appendToLog("ProcessFlowInit.getProcessTaskAndLock() => [Get Data Failed] Exception:" + e.toString(),
					GlobalInfo.EXCEPTION_MODE);
			
			// 如果获取记录过程遇到错误，则关闭数据库连接
			release();		
		} finally {
			try {
				dcETLHelper.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		/////////////////////////////////////
		// Lock data ( PENDING => RUNNING )
		/////////////////////////////////////
		if (dcProcessTaskQueues != null && dcProcessTaskQueues.getTask_queue_id() != null && 
				!"".equals(dcProcessTaskQueues.getTask_queue_id()) && 
						GlobalInfo.DC_QUEUES_PENDING_STS.equals(dcProcessTaskQueues.getStatus_code())) {
			LogManager.appendToLog("Lock data: task_queue_id=" + dcProcessTaskQueues.getTask_queue_id() + 
					" source_sys_key=" + dcProcessTaskQueues.getSource_sys_key() + 
					" process_group_id=" + dcProcessTaskQueues.getProcess_group_id());
			
			// 更新Listener的状态信息(PENDING => RUNNING)
			try {
				new TaskQueueStatusManager(dcETLHelper, 
						                   dcProcessTaskQueues.getTask_queue_id(),  //task_queue_id
						                   dcProcessTaskQueues.getSource_sys_key(),  //source_sys_key
						                   GlobalInfo.DC_QUEUES_TASK_TYPE,  //task_type_code
						                   dcProcessTaskQueues.getProcess_group_id(),  //process_group_id
						                   GlobalInfo.DC_QUEUES_RUNNING_STS,  //status_code
						                   null,  //process_message
						                   "Y",  //start_process_datetime
										   "NULL",  //end_process_datetime
										   "N"  //retry_times_flag
						                   ).updateTaskQueueStatus();
				// Lock success
				flag = true;
				
				// 设置处理状态为running －即锁定
				dcProcessTaskQueues.setStatus_code(GlobalInfo.DC_QUEUES_RUNNING_STS);
				dcProcessTaskQueues.setProcess_message(null);
				
				//======================
				// 添加node节点监控
				//======================
				if ("Y".equals(GlobalInfo.ENABLE_ACTIVITY_NODE_FLAG)) {
					try {
						String activeNodeId = SystemHelper.insertActivityNodes(dcETLHelper, dcProcessTaskQueues.getSource_sys_key(),
																			   dcProcessTaskQueues.getProcess_group_id(), 0, null,
																			   parentNodeId == "-1" ? null : "SOURCE FROM MQ", 
																			   parentNodeId);
						dcProcessTaskQueues.setActive_node_id(activeNodeId);
						LogManager.appendToLog("ProcessFlowInit.setActive_node_id == [ " + dcProcessTaskQueues.getActive_node_id() +" ]");
					} catch (Exception e) {
						e.printStackTrace();
						LogManager.appendToLog("ProcessFlowInit.insertActivityNodes => [Insert Data Failed] Exception:" + e.toString(),
												GlobalInfo.EXCEPTION_MODE);
					}
				}
				
				LogManager.appendToLog("<<Lock successfully>>");
			} catch (Exception e) {
				// 设置process_message但是不更新状态
				//dcProcessTaskQueues.setStatus_code(GlobalInfo.DC_QUEUES_WARNING_STS);
				//dcProcessTaskQueues.setProcess_message("ProcessFlowInit.getProcessTaskAndLock() => [Lock Data Failed] Exception:" + e.toString());
				try {
					dcETLHelper.rollback();
				} catch (SQLException sqle) {
				}
				e.printStackTrace();
				LogManager.appendToLog("ProcessFlowInit.getProcessTaskAndLock() => [Lock Data Failed] Exception:" + e.toString(),
						GlobalInfo.EXCEPTION_MODE);
				
				// 如果获取记录过程遇到错误，则关闭数据库连接
				release();
			} finally {
				try {
					dcETLHelper.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}// if
		
		LogManager.appendToLog("getProcessTaskAndLock => return flag:" + Boolean.toString(flag), GlobalInfo.STATEMENT_MODE);
		LogManager.appendToLog("Leaving getProcessTaskAndLock() ---> ");
		
		return flag;
	}
	
	// 如果遇到错误，则关闭数据库连接
	private void release() {
		try {
			dcETLHelper.closeAll();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
