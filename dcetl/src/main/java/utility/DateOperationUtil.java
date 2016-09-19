package utility;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateOperationUtil {

	/**
	 * 计算两个日期之间相差的天数
	 * 
	 * @param beginDate
	 *            较小的时间
	 * @param endDate
	 *            较大的时间
	 * @return 相差天数
	 * @throws ParseException
	 */
	public static int daysBetween(Date beginDate, Date endDate)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  // new SimpleDateFormat("yyyy-MM-dd");  // 不考虑时分秒的影响
		beginDate = sdf.parse(sdf.format(beginDate));
		endDate = sdf.parse(sdf.format(endDate));
		Calendar cal = Calendar.getInstance();
		cal.setTime(beginDate);
		long time1 = cal.getTimeInMillis();
		
		cal.setTime(endDate);
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}

	/**
	 * 字符串的日期格式的计算
	 */
	public static int daysBetween(String beginDate, String endDate)
			throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  // new SimpleDateFormat("yyyy-MM-dd");  // 不考虑时分秒的影响
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(beginDate));
		long time1 = cal.getTimeInMillis();
		cal.setTime(sdf.parse(endDate));
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}

	/**
	 * @param args
	 * @throws ParseException
	 */
	/*
	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1 = sdf.parse("2016-05-03 10:10:10");
		Date d2 = sdf.parse("2016-05-09 09:00:00");
		System.out.println(daysBetween(d1, d2));

		System.out.println(daysBetween("2012-09-08 10:10:10",
				"2012-09-15 00:00:00"));
	}
	*/

}
