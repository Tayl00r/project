package utility;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import config.GlobalInfo;
import config.GlobalSql;

public class ConcurrentControlManager extends Thread {
	
	private String sourceSysKey;
	private String sourceDpTableOwner;
	private String sourceDpTableName;
	private String destDpTableOwner;
	private String destDpTableName;
	
	private String DP_CONCURRENT_CONTROL_ID;
	private String CONCURRENT_STATUS;
	
	// 构造函数
	public ConcurrentControlManager(String sourceSysKey,
			String sourceDpTableOwner, String sourceDpTableName,
			String destDpTableOwner, String destDpTableName) {
		super();
		this.sourceSysKey = sourceSysKey;
		this.sourceDpTableOwner = sourceDpTableOwner;
		this.sourceDpTableName = sourceDpTableName;
		this.destDpTableOwner = destDpTableOwner;
		this.destDpTableName = destDpTableName;
	}
	
	public ConcurrentControlManager(String DP_CONCURRENT_CONTROL_ID,
			String CONCURRENT_STATUS) {
		super();
		this.DP_CONCURRENT_CONTROL_ID = DP_CONCURRENT_CONTROL_ID;
		this.CONCURRENT_STATUS = CONCURRENT_STATUS;
	}
	
    // 判断是否处于并发处理状态
	public String isConcurrentDpProcessing() 
			throws ClassNotFoundException, SQLException {
		
		LogManager.appendToLog("Entering isConcurrentDpProcessing() ---> ");
        
		String concurrentProcessingId = null;
		
		DBHelper dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		                				    GlobalInfo.DB_DCETL_URL, 
		                				    GlobalInfo.DB_DCETL_USER_NAME,
		                				    GlobalInfo.DB_DCETL_PASSWORD, 
		                				    null);
		
		String[] args = new String[]{ this.sourceSysKey, 
				this.sourceDpTableOwner, this.sourceDpTableName, 
				this.destDpTableOwner, this.destDpTableName};
		
		// 用于接收返回结果集
		String DP_CONCURRENT_CONTROL_ID = null;
	    String CONCURRENT_STATUS = null;
        int DIFF_SECONDS = 0;
        int TIMEOUT_SECONDS = GlobalInfo.CONCURRENT_TIMEOUT_SECONDS;
		
        List<Object> insertArgs = new ArrayList<Object>();
        
		try {
			// execute query
			dcETLHelper.executeQuery(GlobalSql.QUERY_CONCURRENT_CTRL_INFO, (Object[])args);
			
			if (dcETLHelper.resultSet.next()) {
				DP_CONCURRENT_CONTROL_ID = dcETLHelper.resultSet.getString("DP_CONCURRENT_CONTROL_ID");
				CONCURRENT_STATUS = dcETLHelper.resultSet.getString("CONCURRENT_STATUS");
				DIFF_SECONDS = dcETLHelper.resultSet.getInt("DIFF_SECONDS");
				TIMEOUT_SECONDS = dcETLHelper.resultSet.getInt("TIMEOUT_SECONDS");
				
				// 判断是否处于并发状态
				if ( GlobalInfo.CONCURRENT_PENDING_STS.equals(CONCURRENT_STATUS) || 
					 (!GlobalInfo.CONCURRENT_PENDING_STS.equals(CONCURRENT_STATUS) && 
					  DIFF_SECONDS > TIMEOUT_SECONDS )	
					) {
					concurrentProcessingId = DP_CONCURRENT_CONTROL_ID;
					
					// 更新状态
					insertArgs.clear();
					insertArgs.add(GlobalInfo.CONCURRENT_RUNNING_STS);  // running
					insertArgs.add(concurrentProcessingId);
					dcETLHelper.setAndExecuteDML(GlobalSql.UPDATE_DP_CONCURRENT_CTRL, insertArgs);
					
					dcETLHelper.commit();
				}
				
			} else {
				// 不存在记录，添加并发控制记录
				insertArgs.clear();
				concurrentProcessingId = GeneratorUUID.generateUUID();  // id
				insertArgs.add(concurrentProcessingId);
				insertArgs.add(this.sourceSysKey);
				insertArgs.add(this.sourceDpTableOwner);
				insertArgs.add(this.sourceDpTableName);
				insertArgs.add(this.destDpTableOwner);
				insertArgs.add(this.destDpTableName);
				insertArgs.add(GlobalInfo.CONCURRENT_RUNNING_STS);  // running
				insertArgs.add(TIMEOUT_SECONDS);
				// insert
				dcETLHelper.setAndExecuteDML(GlobalSql.INSERT_DP_CONCURRENT_CTRL, insertArgs);
				
				dcETLHelper.commit();
			}
			
		} catch (Exception e) {
			dcETLHelper.rollback();
			e.printStackTrace();
			LogManager.appendToLog("isConcurrentDpProcessing() => Exception: " + e.toString() + " *** " + 
					dcETLHelper.getCurrentErrorSql(), 
					GlobalInfo.EXCEPTION_MODE);
			throw e;
		} finally {
			dcETLHelper.closeAll();
		}
		
		LogManager.appendToLog("Leaving isConcurrentDpProcessing() ---> ");
		
		return concurrentProcessingId;
	}
	
	// 更新状态
	public void run() {
		
		LogManager.appendToLog("Entering Thread => ConcurrentControlManager => run()");
		
		DBHelper dcETLHelper = null;
		try {
			dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
						GlobalInfo.DB_DCETL_URL, 
						GlobalInfo.DB_DCETL_USER_NAME,
						GlobalInfo.DB_DCETL_PASSWORD, 
						null);
		    
			List<Object> args = new ArrayList<Object>();
			// execute query
			args.add(this.CONCURRENT_STATUS);
			args.add(this.DP_CONCURRENT_CONTROL_ID);
			dcETLHelper.setAndExecuteDML(GlobalSql.UPDATE_DP_CONCURRENT_CTRL, args);
			
			dcETLHelper.commit();
			
		} catch (Exception e) {
			try {
				dcETLHelper.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
			LogManager.appendToLog("DpInstanceLogger.run() => Exception: " + e.toString() +  " *** " + 
					dcETLHelper.getCurrentErrorSql(),
					GlobalInfo.EXCEPTION_MODE);
		} finally {
			if (dcETLHelper != null) {
				try {
					dcETLHelper.closeAll();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {}
			} // if
		}
		
		LogManager.appendToLog("Leaving Thread => ConcurrentControlManager => run()");
		
	} // run()
	
}
