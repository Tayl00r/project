package utility;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import java.text.DateFormat;  
import java.text.SimpleDateFormat;

import config.GlobalInfo;

public class DBHelper {
	
	private String dbDriver;
	private String dbUrl;
	private String dbUser;
	private String dbPassWd;
	
	private String currentErrorSql;  // 记录当前发生的sql
	
	public Connection conn;
	public ResultSet resultSet = null;
	public PreparedStatement prepareStatement = null;
	public ResultSet resultSetBatch = null;  // 批量获取的时候存放结果集
	public PreparedStatement prepareStatementBatch = null;  // 批量获取的时候存放pstmt对象
	public Integer batchSize;
	public Integer currentSize; 
	//public static String INSERT_TYPE = "INSERT";
	//public static String UPDATE_TYPE = "UPDATE";
	//public static String QUERY_TYPE = "QUERY";
	//public static String DELETE_TYPE = "DELETE";
	
	
	/**
	  * 获得PreparedStatement向数据库提交的SQL语句
	  **/ 
	private String getPreparedStatementSQL(String sql, Object[] params) {
		
		this.currentErrorSql = sql;
		
		//1 如果没有参数，说明是不是动态SQL语句
	    int paramNum = 0;
	    if (params != null)
	    	paramNum = params.length;
	    if (paramNum < 1) 
	    	return sql;
	    
	    StringBuffer returnSQL = new StringBuffer();
	    try {
		    //2 如果有参数，则是动态SQL语句
		    String[] subSQL = sql.split("\\?");  // 根据？进行拆分SQL，四个问号将会拆分为5个数组
		    
		    //for (int i = 0; i < subSQL.length; i++) {
		    //	System.out.println("subSQL[" + i + "]: " + subSQL[i]);
		    //}
		    
		    for (int i = 0; i < paramNum; i++) {
		    	if (params[i] instanceof Date) {
		    		DateFormat format = new SimpleDateFormat(GlobalInfo.DATE_TIME_FORMAT);
		    		returnSQL.append(subSQL[i]).append(" '").append( format.format((java.util.Date)params[i]) ).append("' ");
		    	} else if (params[i] instanceof Integer) {
		    		returnSQL.append(subSQL[i]).append(" ").append(params[i]).append(" ");
		    	} else if (params[i] == null) {
		    		returnSQL.append(subSQL[i]).append("NULL");
		    	} else {
		    		returnSQL.append(subSQL[i]).append(" '").append(params[i]).append("' ");
		    	}
		    }
		    
		    // 附加最后的尾巴
		    if (subSQL.length > params.length) {
		    	returnSQL.append(subSQL[subSQL.length - 1]);
		    }
		    
	    } catch (Exception e) {
	    	// 该功能仅为了输出调试，如果错误，就不转换了，继续执行程序
	    	returnSQL.append(sql);
	    }
	    
	    this.currentErrorSql = returnSQL.toString();
	    
	    return this.currentErrorSql;
	}
	
	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public Integer getCurrentSize() {
		return currentSize;
	}

	public void setCurrentSize(Integer currentSize) {
		this.currentSize = currentSize;
	}

	public String getCurrentErrorSql() {
		return currentErrorSql;
	}

	public void setCurrentErrorSql(String currentErrorSql) {
		this.currentErrorSql = currentErrorSql;
	}

	//默认:已有连接方式
	public DBHelper(String dbDriver, String dbUrl, String dbUser, String dbPassWd, Integer batchSize) throws ClassNotFoundException, SQLException {
		this.dbDriver = dbDriver;
		this.dbUrl = dbUrl;
		this.dbUser = dbUser;
		this.dbPassWd = dbPassWd;
		this.batchSize = batchSize;
		this.currentSize = 0;
		Class.forName(this.dbDriver);
		this.conn = DriverManager.getConnection(this.dbUrl, this.dbUser, this.dbPassWd);
		this.conn.setAutoCommit(false);  // 默认为true
	}
	
	public void commit() throws SQLException {
		this.conn.commit();
	}
	
	public void rollback() throws SQLException {
		this.conn.rollback();
	}
	
	public Connection getConn() {
		return this.conn;
	}
	
	public void close() throws SQLException {
		
		if (resultSet!=null && !resultSet.isClosed()){
			this.resultSet.close();
		}
		if(prepareStatement!=null && !prepareStatement.isClosed()){
			this.prepareStatement.close();
		}
		if (resultSetBatch!=null && !resultSetBatch.isClosed()){
			this.resultSetBatch.close();
		}
		if(prepareStatementBatch!=null && !prepareStatementBatch.isClosed()){
			this.prepareStatementBatch.close();
		}
		//if(conn!=null && !conn.isClosed()){
		//	this.conn.close();
		//}
	}
	
	public void closeAll() throws SQLException {
		
		if (resultSet!=null && !resultSet.isClosed()){
			this.resultSet.close();
		}
		if(prepareStatement!=null && !prepareStatement.isClosed()){
			this.prepareStatement.close();
		}
		if (resultSetBatch!=null && !resultSetBatch.isClosed()){
			this.resultSetBatch.close();
		}
		if(prepareStatementBatch!=null && !prepareStatementBatch.isClosed()){
			this.prepareStatementBatch.close();
		}
		if(conn!=null && !conn.isClosed()){
			this.conn.close();
		}
	}
	
	// Execute Query according querySql and parameters args
	public void executeQuery(String querySql, Object ...args) throws SQLException{
		
		// 输出sql，用于调试
		//LogManager.appendToLog("args.length=" + args.length, GlobalInfo.DEBUG_MODE);
		LogManager.appendToLog( getPreparedStatementSQL(querySql, args) );
		
		prepareStatement  = conn.prepareStatement(querySql);
		
		if (args != null) {
			for (int i=0; i<args.length; i++){
				int index = i + 1;
	//			if (args[i] instanceof Integer){
	//				prepareStatement.setInt(index, (int) args[i]);
	//			}
	//			else if(args[i] instanceof String){
	//				prepareStatement.setString(index, (String) args[i]);
	//			}
				if (args[i] == null) {
					prepareStatement.setNull(index, Types.NULL);
				} else {
					prepareStatement.setObject(index, args[i]);
				}
			}
		}
				
		resultSet = prepareStatement.executeQuery();	
	}
	
	// Execute Query according querySql and parameters args
	public void executeQueryBatch(String querySql, Object ...args) throws SQLException{
			
		// 输出sql，用于调试
		LogManager.appendToLog( getPreparedStatementSQL(querySql, args) );
			
		prepareStatementBatch  = conn.prepareStatement(querySql
				,ResultSet.TYPE_FORWARD_ONLY
				,ResultSet.CONCUR_READ_ONLY
				);
		
		if (args != null) {
			for (int i=0; i<args.length; i++){
				int index = i + 1;
//				if (args[i] instanceof Integer){
//					prepareStatementBatch.setInt(index, (int) args[i]);
//				}
//				else if(args[i] instanceof String){
//					prepareStatementBatch.setString(index, (String) args[i]);
//				}
				if (args[i] == null) {
					prepareStatementBatch.setNull(index, Types.NULL);
				} else {
					prepareStatementBatch.setObject(index, args[i]);
				}
			}
		}
		
		// 解决数据量获取太大报内存溢出的bug( Java heap space (java.lang.OutOfMemoryError) )
		prepareStatementBatch.setFetchSize(Integer.MIN_VALUE);
		prepareStatementBatch.setFetchDirection(ResultSet.FETCH_FORWARD);
				
		resultSetBatch = prepareStatementBatch.executeQuery();	
	}
	
	public void prepareSql(String insertSql) throws SQLException{
		prepareStatement  = conn.prepareStatement(insertSql);
		currentSize = 0;
		
		try {
			setCurrentErrorSql(insertSql);
		} catch (Exception e) {
		}
	}
	
	public int setAndExecuteDML(List<Object> params) throws SQLException{
		for(int i=0; i<params.size(); i++){
			int index = i + 1;
			if (params.get(i) == null){
				prepareStatement.setNull(index, Types.NULL);
			}
			else{
				prepareStatement.setObject(index, params.get(i) );
			}
		}
		return prepareStatement.executeUpdate();
	}
	
	// 重载
	public int setAndExecuteDML(String insertSql, List<Object> params) throws SQLException{
		
		// 输出sql，用于调试
		LogManager.appendToLog( getPreparedStatementSQL(insertSql, params.toArray()) );
				
		prepareSql(insertSql);
		
		for(int i=0; i<params.size(); i++){
			int index = i + 1;
			if (params.get(i) == null){
				prepareStatement.setNull(index, Types.NULL);
			}
			else{
				prepareStatement.setObject(index, params.get(i) );
			}
		}
		return prepareStatement.executeUpdate();
	}
	
	// Execute Insert Parameters
	public void setDataInsertParams(List<Object> params) throws SQLException{
		for(int i=0; i<params.size();i++){
			int index = i + 1;
			if (params.get(i) == null){
				prepareStatement.setNull(index, Types.NULL);
				prepareStatement.setNull(index+params.size(), Types.NULL);
			}
			else{
//				if (params.get(i) instanceof Integer){
//					prepareStatement.setInt(index, (int) params.get(i) );
//					prepareStatement.setInt(index+params.size(), (int) params.get(i) );
//				}
//				else if(params.get(i)  instanceof String){
//					prepareStatement.setString(index, (String) params.get(i) );
//					prepareStatement.setString(index+params.size(), (String) params.get(i) );
//				}
				prepareStatement.setObject(index, params.get(i) );
				prepareStatement.setObject(index+params.size(), params.get(i));
			}
		}
	}
	
	
	public void setMSTDataInsertParams(List<Object> params) throws SQLException{
		for(int i=0; i<params.size();i++){
			int index = i + 1;
			if (params.get(i) == null){
				prepareStatement.setNull(index, Types.NULL);
			//	prepareStatement.setNull(index+params.size(), Types.NULL);
			}
			else{
//				if (params.get(i) instanceof Integer){
//					prepareStatement.setInt(index, (int) params.get(i) );
//					prepareStatement.setInt(index+params.size(), (int) params.get(i) );
//				}
//				else if(params.get(i)  instanceof String){
//					prepareStatement.setString(index, (String) params.get(i) );
//					prepareStatement.setString(index+params.size(), (String) params.get(i) );
//				}
				prepareStatement.setObject(index, params.get(i));
				//prepareStatement.setObject(index+params.size(), params.get(i));
			}
		}
	}
	
	public void addBatch() throws SQLException{
		prepareStatement.addBatch();
		currentSize++;
	}
	
	public void exeBatch(int pFlag, boolean commitFlag) throws SQLException{
		if (pFlag == 0) {
			if (currentSize != null && batchSize != null && batchSize.equals(currentSize)){
				prepareStatement.executeBatch();
				if (commitFlag) {
					commit();
				}
				currentSize = 0;
			}
		} else {
			if  (currentSize != null && currentSize > 0){
				prepareStatement.executeBatch();
				if (commitFlag) {
					commit();
				}
				currentSize = 0;
			}
		}
	}
	
	public void clearBatch() throws SQLException {
		if(prepareStatement != null && !prepareStatement.isClosed()){
			this.prepareStatement.clearBatch();
		}
	}
	
}
