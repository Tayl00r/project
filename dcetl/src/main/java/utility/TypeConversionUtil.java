package utility;

//import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import config.GlobalInfo;

public class TypeConversionUtil {
	
	public static Object colTypeCheck(String type, String origValue) throws ParseException {
		Object result = null;
		String value = origValue.trim();
		
		if (value == null || value.length() == 0) {
			result = null;
		} else {
			Object fileValue = null;
			if (type.equals("S")) {
				fileValue = value.trim();
			} else if (type.equals("F")) {
				fileValue = Double.valueOf(value);
			} else if (type.equals("D")) {
				fileValue = dateCheck(value);
			} else if (type.equals("N")) {
				fileValue = Double.valueOf(value);
			} else if(type.equals("Y")){
				if(value.equals("0000")){
					fileValue = null;
				}else{
					fileValue =  Double.valueOf(value);
				}
			}
			result = fileValue;
		}
		return result;
	}
	
	// 字符 => 日期
	public static Date dateCheck(String value) throws ParseException{
		Date fileValue = null;
		
		if (value != null && !"".equals(value)) {
			int length = value.length();
			
			if (length == 8) {
				fileValue = new SimpleDateFormat(
						GlobalInfo.SIMPLE_DATE_FORMAT)
						.parse(value);
			} else if (length == 10) {
				fileValue = new SimpleDateFormat(GlobalInfo.DATE_FORMAT)
						.parse(value);
			} else if (length == 19) {
				fileValue = new SimpleDateFormat(
						GlobalInfo.DATE_TIME_FORMAT)
						.parse(value);
			}
			
			if (fileValue != null){
				Date format = new SimpleDateFormat(GlobalInfo.SIMPLE_DATE_FORMAT).parse("1000-01-01");
				if (fileValue.getTime() < format.getTime()){
					fileValue = null;
				}
			}
		}
		
		return fileValue;
	}
	
	// 日期 => 字符
	public static String dateToString(Date date) {
		if (date != null) {
			//return DateFormat.getDateTimeInstance().format(date);
			return new SimpleDateFormat(GlobalInfo.DATE_TIME_FORMAT).format(date);
		} else {
			return null;
		}
	}
	
	// List<Object> => Object[]
	public static Object[] listObjectToObjectArray(List<Object> objectList) {
		int size = 0;
		if (objectList != null && objectList.size() > 0) {
			size = objectList.size();
			
			Object[] objArray = new Object[size];
			for (int i=0; i<size; i++) {
				objArray[i] = objectList.get(i);
			}
			return objArray;
			
		} else {
			return null;
		}
	}
	
}
