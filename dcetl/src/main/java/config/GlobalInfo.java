package config;

import java.io.IOException;
import java.util.Properties;

import entity.DcSysDbConnMapping;
import utility.DBHelper;
import utility.LogManager;

public class GlobalInfo {
	
	// 日期格式
	public static final String SIMPLE_DATE_FORMAT = "yyyyMMdd";
	public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	public static final String DATE_FORMAT_NUM = "yyyyMMddHHmmss";
	
	// service 返回状态
	public static final String SERVICES_RET_SUCCESS = "SUCCESS";
	public static final String SERVICES_RET_ERROR = "ERROR";
	public static final String SERVICES_RET_WARN = "WARN";
	public static final String SERVICES_RET_SEPARATOR = "|::|";
	
	// Loopup type
	public static final String LOOKUP_TYPE_DCETL_SOUR_SYS_KEY = "DCETL_SOUR_SYS_KEY";
		
	// Engine Action Flag
	public static final String DCETL_ENGIN_VALIDATION_RULES = "VALIDATION_RULES";
	public static final String DCETL_ENGIN_DO_PURGE = "DO_PURGE";
	
	// 表别名定义
	public static final String SOUR_TAB_ALIAS_NAME = "sour_t";  // 源表列名
	public static final String DEST_TAB_ALIAS_NAME = "dest_t";  // 目标表列名
	
	// 调试模式
	public static final int EXCEPTION_MODE = 1;
	public static final int DEBUG_MODE = 2;
	public static final int STATEMENT_MODE = 3;
	
	// 设置日期为 now() 标识
	public static final String NOW_DATE_VAL_MODE= "[$NOW()$]";
	public static final String OTHERS_DATE_VAL_MODE= "OTHERS";
	
	// 并发控制状态
	public static final String CONCURRENT_PENDING_STS = "PENDING";
	public static final String CONCURRENT_RUNNING_STS = "RUNNING";  // 运行中
	// 并发超时（单位:秒s）
	public static final int CONCURRENT_TIMEOUT_SECONDS = 7200;  // 2小时=2*60*60
	//多线程等待时间（单位:秒s）
	public static final int MULTI_THREAD_TEST_PER_SECONDS = 10;  // 10s
	// 分批提交数量
	public static final int MAX_BATCH_SIZE = 2000;
	// 最大记录间隔天数
	public static final int MAX_RECORDS_INTERVAL_DAYS = 7;
	
	// 最大获取任务队列尝试次数(0->1->2 最大尝试3次)
	public static final int MAX_ACHIVE_TASK_RETRY_TIMES = 2;
		
	// 回车换行分隔符
	public static final String LINE_SEPARATOR = System.getProperty("line.separator");
	
	// monitor task queue 表信息
	public static final String DC_QUEUE_DETAILS_TYPE = "DCETL_TAB_NAME";
	public static final String DC_QUEUES_TASK_TYPE = "DCETL";
		
	public static final String DC_QUEUES_PENDING_STS = "PENDING";
	public static final String DC_QUEUES_RUNNING_STS = "RUNNING";
	public static final String DC_QUEUES_SUCCESS_STS = "SUCCESS";
	public static final String DC_QUEUES_ERROR_STS = "ERROR";
	public static final String DC_QUEUES_WARNING_STS = "WARNING";  // 警告，仅用于流程状态控制，最终不更新到表
	
	// MQ: all2dc消息队列名称
	public static final String DC2DCETL_MQ_NAME = "DC2DCETL-HSCF-DataPurge-PRODUCTION";  //数据导入后发送到DCETL的消息MQ NAME
	public static String DCETL2RMS_MQ_NAME = null;
	public static String DCETL2DW_MQ_NAME = null;
	
	// rms类型
	public static final String DC2RMS_DCDATAENTRY = "DCDATAENTRY";
	
	//阳关包装特有逻辑
	public static final String SCP_ETL_DB_NAME = "etl_sunshinecpc";
	public static final String SCP_SOURCE_SYS_KEY = "sunshinecpc";
	public static final String SCP_DCETL_INVOICE_HEADERS = "invoice_headers";
	public static final String SCP_DCETL_INVOICE_LINES = "invoice_lines";
	public static final String SCP_DCETL_PO_HEADERS_ALL = "po_headers_all";
	public static final String SCP_ORDER_STS_APPROVED = "APPROVED";
	public static final String SCP_ORDER_STS_OTHER = "OTHER";
	
	// 数据库操作相关用户owner
	public static String SOUR_DB_OWNER = null;
	public static String DEST_DB_OWNER = null;
	public static String DB_RMS_OWNER = null;  //rms
	public static String DB_MONITOR_OWNER = null;  //monitor
	
	// 从配置文件中加载的数据库连接信息
	// DCETL
	public static String DB_DCETL_DRIVER = null;
	public static String DB_DCETL_URL = null;
	public static String DB_DCETL_USER_NAME = null;
	public static String DB_DCETL_PASSWORD = null;
	
	// SRM
	public static String DB_SRM_DRIVER = null;
	public static String DB_SRM_URL = null;
	public static String DB_SRM_USER_NAME = null;
	public static String DB_SRM_PASSWORD = null;
	
	// Enable debug flag (Default: Y )
	public static String ENABLE_DEBUG_FLAG = "Y";
	public static int DEBUG_LEVEL = DEBUG_MODE;  // default debug mode
	
	// 启动activity_node节点监控程序
	public static String ENABLE_ACTIVITY_NODE_FLAG = "Y";
	
	//读取配制文件标识
	public static boolean IS_LOAD_PROPERTIES_FLAG = false;
	
	//数据库分库分表连接信息映射
	public static String DB_DYNAMIC_CONN_URL = null;
	
	///////////////////////////////////////////////////////////////////////////
	// 初始化全局变量
	///////////////////////////////////////////////////////////////////////////
	public static void init() throws IOException {
		
		LogManager.appendToLog("============================================================================", STATEMENT_MODE);
		
		if(!IS_LOAD_PROPERTIES_FLAG) {
		
			Properties pps = new Properties();
			pps.load(GlobalInfo.class
					.getResourceAsStream("../db_config.properties"));
			
			// enable debug flag: ENABLE_DEBUG_FLAG
			if (System.getenv("ENABLE_DEBUG_FLAG") != null) {
				ENABLE_DEBUG_FLAG = System.getenv("ENABLE_DEBUG_FLAG");
				LogManager.appendToLog("成功读取环境变量值ENABLE_DEBUG_FLAG:" + ENABLE_DEBUG_FLAG, DEBUG_MODE);
			} else {
			    if (pps.containsKey("ENABLE_DEBUG_FLAG")) {
			    	ENABLE_DEBUG_FLAG = pps.getProperty("ENABLE_DEBUG_FLAG");
			    	LogManager.appendToLog("成功从启动配置文件中读取ENABLE_DEBUG_FLAG:" + ENABLE_DEBUG_FLAG, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置ENABLE_DEBUG_FLAG", DEBUG_MODE);
				}
			}
			if(System.getenv("DEBUG_LEVEL") != null) {
				DEBUG_LEVEL = Integer.parseInt(System.getenv("DEBUG_LEVEL"));
		    	LogManager.appendToLog("成功读取环境变量值DEBUG_LEVEL:" + DEBUG_LEVEL, DEBUG_MODE);
			} else {
			    if (pps.containsKey("DEBUG_LEVEL")) {
			    	DEBUG_LEVEL = Integer.parseInt(pps.getProperty("DEBUG_LEVEL"));
			    	LogManager.appendToLog("成功从启动配置文件中读取DEBUG_LEVEL:" + DEBUG_LEVEL, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置DEBUG_LEVEL", DEBUG_MODE);
				}
			}
		    
			//Dynamic DB CONN URL
			if(System.getenv("DB_DYNAMIC_CONN_URL") != null) {
				DB_DYNAMIC_CONN_URL = System.getenv("DB_DYNAMIC_CONN_URL");
				LogManager.appendToLog("成功读取环境变量值DB_DYNAMIC_CONN_URL:" + DB_DYNAMIC_CONN_URL, DEBUG_MODE);
			} else {
				if (pps.containsKey("DB_DYNAMIC_CONN_URL")) {
					if (DB_DYNAMIC_CONN_URL == null) {
						DB_DYNAMIC_CONN_URL = pps.getProperty("DB_DYNAMIC_CONN_URL");
					}
					LogManager.appendToLog("成功从启动配置文件中读取DB_DYNAMIC_CONN_URL:" + DB_DYNAMIC_CONN_URL, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置DB_DYNAMIC_CONN_URL", DEBUG_MODE);
				}
			}
			
			// DCETL DB CONNECTION
			if (pps.containsKey("DB_DCETL_DRIVER")) {
				if (DB_DCETL_DRIVER == null) {
					DB_DCETL_DRIVER = pps.getProperty("DB_DCETL_DRIVER");
				}
				LogManager.appendToLog("成功从启动配置文件中读取DB_DCETL_DRIVER:" + DB_DCETL_DRIVER, DEBUG_MODE);
			} else {
				LogManager.appendToLog("从启动配置文件中找不到配置DB_DCETL_DRIVER", DEBUG_MODE);
			}
			if(System.getenv("DB_ENV_DCETL_URL") != null) {
				DB_DCETL_URL = System.getenv("DB_ENV_DCETL_URL");
				LogManager.appendToLog("成功读取环境变量值DB_ENV_DCETL_URL:" + DB_DCETL_URL, DEBUG_MODE);
			} else {
				if (pps.containsKey("DB_DCETL_URL")) {
					if (DB_DCETL_URL == null) {
						DB_DCETL_URL = pps.getProperty("DB_DCETL_URL");
					}
					LogManager.appendToLog("成功从启动配置文件中读取DB_DCETL_URL:" + DB_DCETL_URL, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置DB_DCETL_URL", DEBUG_MODE);
				}
			}
			if(System.getenv("DB_ENV_DCETL_USER_NAME") != null) {
				DB_DCETL_USER_NAME = System.getenv("DB_ENV_DCETL_USER_NAME");
				LogManager.appendToLog("成功读取环境变量值DB_ENV_DCETL_USER_NAME:" + DB_DCETL_USER_NAME, DEBUG_MODE);
			} else {
				if (pps.containsKey("DB_DCETL_USER_NAME")) {
					if (DB_DCETL_USER_NAME == null) {
						DB_DCETL_USER_NAME = pps.getProperty("DB_DCETL_USER_NAME");
					}
					LogManager.appendToLog("成功从启动配置文件中读取DB_DCETL_USER_NAME:" + DB_DCETL_USER_NAME, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置DB_DCETL_USER_NAME", DEBUG_MODE);
				}
			}
			if(System.getenv("DB_ENV_DCETL_PASSWORD") != null) {
				DB_DCETL_PASSWORD = System.getenv("DB_ENV_DCETL_PASSWORD");
				LogManager.appendToLog("成功读取环境变量值DB_ENV_DCETL_PASSWORD:" + DB_DCETL_PASSWORD, DEBUG_MODE);
			} else {
				if (pps.containsKey("DB_DCETL_PASSWORD")) {
					if (DB_DCETL_PASSWORD == null) {
						DB_DCETL_PASSWORD = pps.getProperty("DB_DCETL_PASSWORD");
					}
					LogManager.appendToLog("成功从启动配置文件中读取DB_DCETL_PASSWORD:" + DB_DCETL_PASSWORD, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置DB_DCETL_PASSWORD", DEBUG_MODE);
				}
			}
			
			// SRM DB CONNECTION
			/*
		    if (pps.containsKey("DB_SRM_DRIVER")) {
		    	DB_SRM_DRIVER = pps.getProperty("DB_SRM_DRIVER");
		    	LogManager.appendToLog("成功从启动配置文件中读取DB_SRM_DRIVER:" + DB_SRM_DRIVER, DEBUG_MODE);
		    } else {
		    	LogManager.appendToLog("从启动配置文件中找不到配置DB_SRM_DRIVER", DEBUG_MODE);
		    }
		    if (pps.containsKey("DB_SRM_URL")) {
		    	DB_SRM_URL = pps.getProperty("DB_SRM_URL");
		    	LogManager.appendToLog("成功从启动配置文件中读取DB_SRM_URL:" + DB_SRM_URL, DEBUG_MODE);
		    } else {
		    	LogManager.appendToLog("从启动配置文件中找不到配置DB_SRM_URL", DEBUG_MODE);
		    }*/
			if(System.getenv("DB_ENV_SRM_SCHEMA") != null) {
				DB_SRM_USER_NAME = System.getenv("DB_ENV_SRM_SCHEMA");
				LogManager.appendToLog("成功读取环境变量值DB_ENV_SRM_SCHEMA:" + DB_SRM_USER_NAME, DEBUG_MODE);
			} else {
			    if (pps.containsKey("DB_SRM_USER_NAME")) {
			    	if (DB_SRM_USER_NAME == null) {
			    		DB_SRM_USER_NAME = pps.getProperty("DB_SRM_USER_NAME");
			    	}
			    	LogManager.appendToLog("成功从启动配置文件中读取DB_SRM_USER_NAME:" + DB_SRM_USER_NAME, DEBUG_MODE);
			    } else {
			    	LogManager.appendToLog("从启动配置文件中找不到配置DB_SRM_USER_NAME", DEBUG_MODE);
			    }
			}
		    /*
		    if (pps.containsKey("DB_SRM_PASSWORD")) {
		    	DB_SRM_PASSWORD = pps.getProperty("DB_SRM_PASSWORD");
		    	LogManager.appendToLog("成功从启动配置文件中读取DB_SRM_PASSWORD:" + DB_SRM_PASSWORD, DEBUG_MODE);
		    } else {
		    	LogManager.appendToLog("从启动配置文件中找不到配置DB_SRM_PASSWORD", DEBUG_MODE);
		    }*/
		    
		    // rms：获取lookup 信息
			if(System.getenv("DB_ENV_RMS_SCHEMA") != null) {
				DB_RMS_OWNER = System.getenv("DB_ENV_RMS_SCHEMA");
				LogManager.appendToLog("成功读取环境变量值DB_ENV_RMS_SCHEMA:" + DB_RMS_OWNER, DEBUG_MODE);
			} else {
			    if (pps.containsKey("DB_RMS_OWNER")) {
			    	if (DB_RMS_OWNER == null) {
			    		DB_RMS_OWNER = pps.getProperty("DB_RMS_OWNER");
			    	}
			    	LogManager.appendToLog("成功从启动配置文件中读取DB_RMS_OWNER:" + DB_RMS_OWNER, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置DB_RMS_OWNER", DEBUG_MODE);
				}
			}
			
			//monitor：获取queues 信息
			if(System.getenv("DB_ENV_MONITOR_SCHEMA") != null) {
				DB_MONITOR_OWNER = System.getenv("DB_ENV_MONITOR_SCHEMA");
				LogManager.appendToLog("成功读取环境变量值DB_ENV_MONITOR_SCHEMA:" + DB_MONITOR_OWNER, DEBUG_MODE);
			} else {
			    if(pps.containsKey("DB_MONITOR_OWNER")){
			    	if (DB_MONITOR_OWNER == null) {
			    		DB_MONITOR_OWNER = pps.getProperty("DB_MONITOR_OWNER");
			    	}
			    	LogManager.appendToLog("成功从启动配置文件中读取DB_MONITOR_OWNER:" + DB_MONITOR_OWNER, DEBUG_MODE);
			    } else {
			    	LogManager.appendToLog("从启动配置文件中找不到配置DB_MONITOR_OWNER:" + DB_MONITOR_OWNER, DEBUG_MODE);
			    }
			}
		    
		    //dcetl2rms MQ Name
			if(System.getenv("MQ_ENV_DCETL2RMS_QNAME") != null) {
				DCETL2RMS_MQ_NAME = System.getenv("MQ_ENV_DCETL2RMS_QNAME");
				LogManager.appendToLog("成功读取环境变量值MQ_ENV_DCETL2RMS_QNAME:" + DCETL2RMS_MQ_NAME, DEBUG_MODE);
			} else {
			    if(pps.containsKey("ACTIVEMQ_DCETL2RMS_MQ_NAME")){
			    	if (DCETL2RMS_MQ_NAME == null) {
			    		DCETL2RMS_MQ_NAME = pps.getProperty("ACTIVEMQ_DCETL2RMS_MQ_NAME");
			    	}
			    	LogManager.appendToLog("成功从启动配置文件中读取ACTIVEMQ_DCETL2RMS_MQ_NAME:" + DCETL2RMS_MQ_NAME, DEBUG_MODE);
			    } else {
			    	LogManager.appendToLog("从启动配置文件中找不到配置ACTIVEMQ_DCETL2RMS_MQ_NAME:" + DCETL2RMS_MQ_NAME, DEBUG_MODE);
			    }
			}
			
			 //dcetl2dw MQ Name
			if(System.getenv("MQ_ENV_DCETL2DW_QNAME") != null) {
				DCETL2DW_MQ_NAME = System.getenv("MQ_ENV_DCETL2DW_QNAME");
				LogManager.appendToLog("成功读取环境变量值MQ_ENV_DCETL2DW_QNAME:" + DCETL2DW_MQ_NAME, DEBUG_MODE);
			} else {
			    if(pps.containsKey("ACTIVEMQ_DCETL2DW_MQ_NAME")){
			    	if (DCETL2DW_MQ_NAME == null) {
			    		DCETL2DW_MQ_NAME = pps.getProperty("ACTIVEMQ_DCETL2DW_MQ_NAME");
			    	}
			    	LogManager.appendToLog("成功从启动配置文件中读取ACTIVEMQ_DCETL2DW_MQ_NAME:" + DCETL2DW_MQ_NAME, DEBUG_MODE);
			    } else {
			    	LogManager.appendToLog("从启动配置文件中找不到配置ACTIVEMQ_DCETL2DW_MQ_NAME:" + DCETL2DW_MQ_NAME, DEBUG_MODE);
			    }
			}
			
			
			//启动activity_node节点监控程序
			if(System.getenv("ENABLE_ACTIVITY_NODE_FLAG") != null) {
				ENABLE_ACTIVITY_NODE_FLAG = System.getenv("ENABLE_ACTIVITY_NODE_FLAG");
		    	LogManager.appendToLog("成功读取环境变量值ENABLE_ACTIVITY_NODE_FLAG:" + ENABLE_ACTIVITY_NODE_FLAG, DEBUG_MODE);
			} else {
			    if (pps.containsKey("ENABLE_ACTIVITY_NODE_FLAG")) {
			    	ENABLE_ACTIVITY_NODE_FLAG = pps.getProperty("ENABLE_ACTIVITY_NODE_FLAG");
			    	LogManager.appendToLog("成功从启动配置文件中读取ENABLE_ACTIVITY_NODE_FLAG:" + ENABLE_ACTIVITY_NODE_FLAG, DEBUG_MODE);
				} else {
					LogManager.appendToLog("从启动配置文件中找不到配置ENABLE_ACTIVITY_NODE_FLAG", DEBUG_MODE);
				}
			}
		    
		    // from SRM to DCETL
		    if (SOUR_DB_OWNER == null) {
		    	SOUR_DB_OWNER = DB_SRM_USER_NAME;
		    }
		    if (DEST_DB_OWNER == null) {
		    	DEST_DB_OWNER = DB_DCETL_USER_NAME;
		    }
		    
		    // 配制文件已加载
		    IS_LOAD_PROPERTIES_FLAG = true;
		}
	    
	}

}
