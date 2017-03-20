package utility;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import config.GlobalInfo;
import config.GlobalSql;

/*
 * 阳关概念包装的特殊逻辑，当发票数据进入DC时，更新发票对应的订单状态
 * */

public class SCPProcessManager {

	public SCPProcessManager() {
		// TODO Auto-generated constructor stub
	}

	public void process(DBHelper dcETLHelper, String sourceSysKey) throws ClassNotFoundException, SQLException {
		// DBHelper dcETLHelper =
		// if (dcETLHelper == null) {
		//		dcETLHelper = new DBHelper(GlobalInfo.DB_DCETL_DRIVER,
		// 							GlobalInfo.DB_DCETL_URL,
		// 							GlobalInfo.DB_DCETL_USER_NAME,
		// 							GlobalInfo.DB_DCETL_PASSWORD,
		// 							null);
		// }
		
		List<Object> args = new ArrayList<Object>();
		args.add(GlobalInfo.SCP_ORDER_STS_OTHER);
		args.add(GlobalInfo.SCP_ORDER_STS_APPROVED);
		args.add(sourceSysKey);
		
		// 执行状态更新
		dcETLHelper.setAndExecuteDML(GlobalSql.SCP_UPDATE_PO_HEADERS_ORD_STS, args);
		dcETLHelper.commit();
		dcETLHelper.close();
	}
}
