package utility;

import config.GlobalInfo;
import config.GlobalSql;
import entity.DcSysDbConnMapping;

public class DbConnMappingUtil {
	
	// 初始化分库分表数据库连接信息
	public static DcSysDbConnMapping initDbConnMapping(DBHelper dbHelper, String sourceSysKey) throws Exception {

		LogManager.appendToLog("Entering InitSysDbConnMapping(" + sourceSysKey + ") ---> ", GlobalInfo.STATEMENT_MODE);
		
		if (dbHelper == null) {
			dbHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER, GlobalInfo.DB_DCETL_URL,
					GlobalInfo.DB_DCETL_USER_NAME, GlobalInfo.DB_DCETL_PASSWORD, null);
		}
		
		DcSysDbConnMapping sysDbConnMapping = null;
		
		try {
			// execute query
			dbHelper.executeQuery(GlobalSql.QUERY_SYS_DB_CONN_MAPPING, new Object[] { sourceSysKey });
			if (dbHelper.resultSet.next()) {
				sysDbConnMapping = new DcSysDbConnMapping();
				//
				sysDbConnMapping.setSource_sys_key(dbHelper.resultSet.getString("source_sys_key"));
				sysDbConnMapping.setDb_host(dbHelper.resultSet.getString("db_host"));
				sysDbConnMapping.setDb_port(dbHelper.resultSet.getString("db_port"));
				sysDbConnMapping.setDb_schema(dbHelper.resultSet.getString("db_schema"));
				sysDbConnMapping.setDb_conn_user(dbHelper.resultSet.getString("db_conn_user"));
				sysDbConnMapping.setDb_conn_password(dbHelper.resultSet.getString("db_conn_password"));
				// 动态拼接URL
				String dbUrl = GlobalInfo.DB_DYNAMIC_CONN_URL
						.replaceAll("%DB_HOST%", sysDbConnMapping.getDb_host())
						.replaceAll("%DB_PORT%", sysDbConnMapping.getDb_port())
						.replaceAll("%DB_SCHEMA%", sysDbConnMapping.getDb_schema());
				sysDbConnMapping.setDb_url(dbUrl);
			} else {
				// 未找到连接配置信息
				throw new RuntimeException("*** initDbConnMapping('" + sourceSysKey + "') NO_DATA_FOUND *** ");
			}
		} finally {
			dbHelper.close();
		}

		LogManager.appendToLog("Leaving InitSysDbConnMapping() ---> ", GlobalInfo.STATEMENT_MODE);
		
		return sysDbConnMapping;
	}

}
