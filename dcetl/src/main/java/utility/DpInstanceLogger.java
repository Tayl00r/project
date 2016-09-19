package utility;

import java.util.ArrayList;
import java.util.List;

import config.GlobalInfo;
import config.GlobalSql;

public class DpInstanceLogger extends Thread {

	private List<Object> dpInstanceArgs;
	
	
	public DpInstanceLogger(List<Object> pDpInstanceArgs) {
		super();
		this.dpInstanceArgs = new ArrayList<Object>(pDpInstanceArgs);
		LogManager.appendToLog("  DpInstanceLogger => this.dpInstanceArgs.size(): " + this.dpInstanceArgs.size(), 
				GlobalInfo.STATEMENT_MODE);
	}

	public void run() {
		
		LogManager.appendToLog("Entering Thread => DpInstanceLogger => run()");
		LogManager.appendToLog("  run => this.dpInstanceArgs.size(): " + this.dpInstanceArgs.size(), GlobalInfo.STATEMENT_MODE);
		
		DBHelper dcETLHelper = null;
		try {
			dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
						GlobalInfo.DB_DCETL_URL, 
						GlobalInfo.DB_DCETL_USER_NAME,
						GlobalInfo.DB_DCETL_PASSWORD, 
						null);
		
			// execute query
			dcETLHelper.setAndExecuteDML(GlobalSql.INSERT_DP_INSTANCE_LOG, this.dpInstanceArgs);
			
			dcETLHelper.commit();
			
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog("DpInstanceLogger.run() => Exception: " + e.toString() + " *** " + dcETLHelper.getCurrentErrorSql(),
					GlobalInfo.EXCEPTION_MODE);
		} finally {
			if (dcETLHelper != null) {
				try {
					dcETLHelper.rollback();
					dcETLHelper.closeAll();
				} catch (Exception e) {
					e.printStackTrace();
				} finally {}
			} // if
		}
		
		LogManager.appendToLog("Leaving Thread => DpInstanceLogger => run()");
		
	} // run()
		
}
