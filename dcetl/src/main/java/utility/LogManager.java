package utility;

import org.apache.log4j.Logger;

import config.GlobalInfo;

public class LogManager {
    
	private static Logger logger = Logger.getLogger(LogManager.class);
	
	public static void appendToLog(String logMessage) {
		int debugModeLevel = GlobalInfo.DEBUG_MODE;
		if ("Y".equalsIgnoreCase(GlobalInfo.ENABLE_DEBUG_FLAG) && 
				debugModeLevel <= GlobalInfo.DEBUG_LEVEL) {
			//System.out.println(logMessage);
			logger.info(logMessage);
		}
	}
	
	public static void appendToLog(String logMessage, int debugModeLevel) {
		if ("Y".equalsIgnoreCase(GlobalInfo.ENABLE_DEBUG_FLAG) && 
				debugModeLevel <= GlobalInfo.DEBUG_LEVEL) {
			//System.out.println(logMessage);
			logger.info(logMessage);
		}
	}
}
