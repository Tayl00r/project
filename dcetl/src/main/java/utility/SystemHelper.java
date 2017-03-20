package utility;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import config.GlobalInfo;
import config.GlobalSql;

public class SystemHelper {
	
	
	public static final String STR_VENDOR = "VENDOR";
	public static final String STR_LE = "LE";
	public static final String STR_VDR_LE = "VDR-LE";
	public static final String STR_OU = "OU";
	
	// 修改details的process_flag (N ==> Y)
	public static void updateQueuesDetailsProcessStatus(DBHelper dcETLHelper, List<Object> args) throws SQLException {
		
		try {
			// 修改process_flag( N ==> Y)
			dcETLHelper.setAndExecuteDML(GlobalSql.UPDATE_DC_DETAILS_PROCESS_FLAG, args);
			//dcETLHelper.commit();
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("[Warning]SystemHelper.updateQueuesDetailsProcessStatus() => Exception:" + e.toString(), 
									GlobalInfo.EXCEPTION_MODE);
		} finally {
			dcETLHelper.close();
		}
		
	}

	// 处理监听到的信息
	public static List<String> getListenerMessage(String[] dc2dcetl_infos) throws ClassNotFoundException, SQLException {
		DBHelper dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
											GlobalInfo.DB_DCETL_URL, 
											GlobalInfo.DB_DCETL_USER_NAME,
											GlobalInfo.DB_DCETL_PASSWORD, 
											null);
		LogManager.appendToLog("Entering getListenerMessage() ---> ");
		for (int i=0; i<dc2dcetl_infos.length; i++){
			LogManager.appendToLog("dc2dcetl_infos["+ i+ "]:" + dc2dcetl_infos[i], GlobalInfo.DEBUG_MODE);
		}
		
		List<String> sourceSysKeyList = new ArrayList<String>();
		try {
			String[] queryArgs = new String[] { dc2dcetl_infos[0],
					                   GlobalInfo.DC_QUEUES_TASK_TYPE,
									   GlobalInfo.DC_QUEUES_PENDING_STS,
									   GlobalInfo.DC_QUEUE_DETAILS_TYPE, 
									   dc2dcetl_infos[1] };
			dcETLHelper.executeQuery(GlobalSql.QUERY_LISTENER_SOURCE_SYS_KEY_TAB, (Object[]) queryArgs);
			while (dcETLHelper.resultSet.next()) {
				sourceSysKeyList.add(dcETLHelper.resultSet.getString("DCETL_TAB_INFO"));
			}

			// 更新Listener的状态信息(PENDING => RUNNING)
			if (sourceSysKeyList != null && sourceSysKeyList.size() > 0) {
				List<Object> args = new ArrayList<Object>();
				args.add(GlobalInfo.DC_QUEUES_RUNNING_STS);
				args.add(GlobalInfo.DC_QUEUES_TASK_TYPE);// task_type_code
				args.add(dc2dcetl_infos[0]);// 加入source_sys_key
				args.add(dc2dcetl_infos[1]);// process_group_id
				dcETLHelper.setAndExecuteDML(GlobalSql.UPDATE_DP_TASK_QUEUES_STATUS, args);
				dcETLHelper.commit();
				args.clear();
			}

		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("SystemHelper.getListenerMessage() => Exception:" + e.toString(), 
									GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.closeAll();
		}

		LogManager.appendToLog("sourceSysKeyList.size() => " + sourceSysKeyList.size());
		
		return sourceSysKeyList;
	}
			
		
	// 修改queues为success，迁移到log
	public static boolean transferQueuesToLogAndDelete(DBHelper dcETLHelper, String[] dc2dcetl_infos) 
			throws ClassNotFoundException, SQLException {
		boolean flag = false;
		//DBHelper dcETLHelper = 
		//if (dcETLHelper == null) {
		//	dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		//									GlobalInfo.DB_DCETL_URL, 
		//									GlobalInfo.DB_DCETL_USER_NAME,
		//									GlobalInfo.DB_DCETL_PASSWORD, 
		//									null);
		//}
		List<Object> args = new ArrayList<Object>();
		try {
			// 迁移queues ==> queues_logs
			args.add(GlobalInfo.DC_QUEUES_TASK_TYPE);
			args.add(dc2dcetl_infos[3]);  //status_code
			args.add(dc2dcetl_infos[1]);  //process_group_id
			args.add(dc2dcetl_infos[0]);  //source_sys_key
			dcETLHelper.setAndExecuteDML(GlobalSql.MOVE_QUEUES_2_QUEUES_LOGS, args);
			// 删除quenes表中的数据
			dcETLHelper.setAndExecuteDML(GlobalSql.DELETE_TASK_QUEUES_INFO, args);

			dcETLHelper.commit();
			args.clear();
			flag = true;
		} catch (Exception e) {
			dcETLHelper.rollback();
			e.printStackTrace();
			LogManager.appendToLog("SystemHelper.transferQueuesToLogAndDelete() => Exception:" + e.toString(), 
									GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
		return flag;
	}
		
	// 将details迁移到log
	public static boolean transferQueuesDetailsAndDelete(DBHelper dcETLHelper, String processGroupId) throws ClassNotFoundException, SQLException {
		boolean flag = false;
		//DBHelper dcETLHelper = 
		//if (dcETLHelper == null) {
		//	dcETLHelper	= new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		//									GlobalInfo.DB_DCETL_URL, 
		//									GlobalInfo.DB_DCETL_USER_NAME,
		//									GlobalInfo.DB_DCETL_PASSWORD, 
		//									null);
		//}
		List<Object> args = new ArrayList<Object>();
		try {
			// 迁移details ==> details_logs
			args.add(GlobalInfo.DC_QUEUE_DETAILS_TYPE);
			args.add(processGroupId);
			dcETLHelper.setAndExecuteDML(GlobalSql.MOVE_DETAILS_2_DETAILS_LOGS, args);
			// 删除details表中的数据
			dcETLHelper.setAndExecuteDML(GlobalSql.DELETE_TASK_QUEUES_DETAILS_INFO, args);

			dcETLHelper.commit();
			args.clear();
			flag = true;
		} catch (Exception e) {
			dcETLHelper.rollback();
			e.printStackTrace();
			LogManager.appendToLog("SystemHelper.transferQueuesDetailsAndDelete() => Exception:" + e.toString(), 
									GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.close();
		}
		return flag;
	}
	
	// activity_nodes
	public static String insertActivityNodes(DBHelper dcETLHelper, 
											 String sourceSysKey, String processBatchNo, int nodeSeq,
											 String nodeMessage, String description, String parentNodeId) 
					throws SQLException, ClassNotFoundException {

		if (dcETLHelper == null) {
		 //DBHelper dcETLHelper = 
				dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER, 
											GlobalInfo.DB_DCETL_URL,
											GlobalInfo.DB_DCETL_USER_NAME, 
											GlobalInfo.DB_DCETL_PASSWORD, null);
		}
		
		String uuid = null;
		try {
			dcETLHelper.prepareSql(GlobalSql.MONITOR_ACTIVITY_NODES_INSERT);
			List<Object> params = new ArrayList<Object>();
			uuid = GeneratorUUID.generateUUID();
			params.add(uuid);
			params.add(processBatchNo);
			params.add(sourceSysKey);
			params.add("3");
			params.add("DCETL");
			params.add(getNodeStatus(nodeSeq)); 					 // node_status
			params.add(nodeMessage);
			params.add(parentNodeId == null ? "-1" : parentNodeId);						    	 // parent_node_id
			params.add(description == null ? "NODE FROM DCETL" : description);
			params.add(0);
			dcETLHelper.setAndExecuteDML(params);
			dcETLHelper.commit();
			return uuid;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		} finally {
			dcETLHelper.close();
		}

	}
	
	public static void updateNodes(String nodeId, int nodeSeq, String nodeDesc) throws SQLException, ClassNotFoundException {
		DBHelper dbHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER, GlobalInfo.DB_DCETL_URL,
				GlobalInfo.DB_DCETL_USER_NAME, GlobalInfo.DB_DCETL_PASSWORD, null);
		PreparedStatement prepareStatement = null;
		try {
			prepareStatement = dbHelper.conn.prepareStatement(GlobalSql.MONITOR_ACTIVITY_UPDATE);
			prepareStatement.setString(1, getNodeStatus(nodeSeq));
			prepareStatement.setString(2, nodeDesc);
			prepareStatement.setString(3, nodeId);
			prepareStatement.executeUpdate();
			dbHelper.commit();
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("SystemHelper.updateNodes() => Exception:" + e.toString(), 
					GlobalInfo.EXCEPTION_MODE);
		} finally {
			dbHelper.closeAll();
		}
	}

	public static String getNodeStatus(int nodeSeq) {
		if (nodeSeq == 0) {
			return GlobalInfo.DC_QUEUES_RUNNING_STS;
		} else if (nodeSeq == 1) {
			return GlobalInfo.DC_QUEUES_SUCCESS_STS;
		} else if (nodeSeq == 00){
			return null;
		}
		return GlobalInfo.DC_QUEUES_ERROR_STS;
	}
	

}
