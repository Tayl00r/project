package dcetl;

import java.util.Date;

import utility.GeneratorUUID;
import utility.TypeConversionUtil;
import iface.impl.DataPurgeService;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println( GeneratorUUID.generateUUID() );
		
		
		String sourceSysKey = "highly";
		String sourceTabOwner = "srmtest";
		String sourceTabName = "transaction_lines";
		String dpStartDate = "2016-01-01 00:00:00";
		String dpEndDate = "2016-05-20 00:00:00";
		String initialDataImpFlag = "Y";
		
		// 数据清洗
		
		String returnVal = new DataPurgeService().startDpService(sourceSysKey, 
				sourceTabOwner, 
				sourceTabName, 
				dpStartDate, 
				dpEndDate,
				initialDataImpFlag);
		System.out.println(returnVal);
		
		
	    /*
		Date currentSysDatetime = new java.sql.Date(new Date().getTime());  // get current sysdate
		String returnVal1 = TypeConversionCheck.dateToString(currentSysDatetime);
		
		System.out.println(returnVal1);
		*/
		
		// 校验sql
		/*
		String returnVal = new DataPurgeService().validateDpRulesService(sourceSysKey, 
				sourceTabOwner, sourceTabName);
		System.out.println(returnVal);
		*/
		
		
		//String text = "ifnull(?,ifnull(date_add(?, interval -1*? second),DC_LAST_UPDATE_DATE)) <= DC_LAST_UPDATE_DATE";
		//text = text.replaceAll("[^*_][DC_LAST_UPDATE_DATE][^*_]", " invl.DC_LAST_UPDATE_DATE ");
		//System.out.println(text); 
		
	}

}
