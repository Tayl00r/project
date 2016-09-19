package dcetl;

import iface.impl.DataPurgeService;

public class Test2 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String sourceSysKey = "solareast";
		String sourceTabOwner = "srm";
		String sourceTabName = "transaction_lines";
		//String dpStartDate = null; //"2014-05-01 00:00:00";
		//String dpEndDate = null;  //"2016-05-20 00:00:00";
		//String initialDataImpFlag = "N";
		
		// 数据清洗验证，校验sql
		
		String returnVal = new DataPurgeService().validateDpRulesService(sourceSysKey, 
				sourceTabOwner, sourceTabName);
		System.out.println(returnVal);
		
		
		
		
	}

}
