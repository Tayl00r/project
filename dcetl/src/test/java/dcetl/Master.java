package dcetl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.mysql.jdbc.Statement;

import utility.DBHelper;

public class Master {

	public static void main(String[] args) throws SQLException, ClassNotFoundException {

		Runtime rt = Runtime.getRuntime();

		System.out.println("1 Total Memory= " + rt.totalMemory() + // 打印总内存大小
				" Free Memory = " + rt.freeMemory()); // 打印空闲内存大小

		try {
		
			DBHelper dbHelper = new DBHelper("com.mysql.jdbc.Driver",
					"jdbc:mysql://172.20.0.161:3306/dcetl?useUnicode=true&characterEncoding=UTF-8", 
					"root", "handhand", 0);
			com.mysql.jdbc.Statement stmt = (Statement) dbHelper.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY);
	
			stmt.setFetchSize(Integer.MIN_VALUE);
			stmt.enableStreamingResults();
	
			ResultSet rs = stmt.executeQuery("select * from dp_rules_field_mapping_configs t"
					+ " where t.DP_RULES_MAPPING_CONFIG_ID = 'ndfiwqhro2urpu21093849ufjwnoab' "
					+ "and t.COLUMN_MAPPING_TO_VALUE = 'STANDARD' "
					+ " for update ");
			
			// dbHelper.executeQuery("SELECT * from srm.invoice_lines"
			// + " limit 100000");
			System.out.println("2 Total Memory= " + rt.totalMemory() + // 打印总内存大小
					" Free Memory = " + rt.freeMemory()); // 打印空闲内存大小
			while(rs.next())
				System.out.println("value=" + rs.getString(1));
			
			rs.close();
			System.out.println("close");
			stmt.close();
			System.out.println("close1");
			dbHelper.close();
			// System.gc();
        
		} catch (com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException e) {
			System.out.println("com.mysql.jdbc.exceptions.jdbc4.MySQLSyntaxErrorException: >>");
			System.out.println(e.toString());
		} catch (com.mysql.jdbc.exceptions.jdbc4.MySQLTimeoutException e) {
			System.out.println("com.mysql.jdbc.exceptions.jdbc4.MySQLTimeoutException: >>");
			System.out.println(e.toString());
		} catch (com.mysql.jdbc.exceptions.jdbc4.MySQLQueryInterruptedException e) {
			System.out.println("com.mysql.jdbc.exceptions.jdbc4.MySQLQueryInterruptedException: >>");
			System.out.println(e.toString());
		} catch (Exception e) {
			System.out.println(e.toString());
		}
			
		System.out.println("3 Total Memory= " + rt.totalMemory() + // 打印总内存大小
				" Free Memory = " + rt.freeMemory()); // 打印空闲内存大小
	}
}