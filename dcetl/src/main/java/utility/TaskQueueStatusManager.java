package utility;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import config.GlobalSql;

public class TaskQueueStatusManager {
	
	private DBHelper dcETLHelper;
	
	private String task_queue_id;
	private String source_sys_key;
	private String task_type_code;
	private String process_group_id;
	private String status_code;
	private String process_message;
	private String start_process_datetime_flag;
	private String end_process_datetime_flag;
	
	public TaskQueueStatusManager(DBHelper dcETLHelper, String task_queue_id, String source_sys_key,
			String task_type_code, String process_group_id, String status_code, String process_message,
			String start_process_datetime_flag, String end_process_datetime_flag) {
		super();
		this.dcETLHelper = dcETLHelper;  // 数据库连接串
		this.task_queue_id = task_queue_id;
		this.source_sys_key = source_sys_key;
		this.task_type_code = task_type_code;
		this.process_group_id = process_group_id;
		this.status_code = status_code;
		this.process_message = process_message;
		this.start_process_datetime_flag = start_process_datetime_flag;
		this.end_process_datetime_flag = end_process_datetime_flag;
	}
	
	// 更新任务表记录
	public void updateTaskQueueStatus() throws SQLException {
		if (dcETLHelper != null) {
			List<Object> statusArgs = new ArrayList<Object>();
			statusArgs.add(this.status_code);  //status_code
			if(this.process_message != null && this.process_message.length() > 2000) {
				statusArgs.add(this.process_message.substring(0, 2000));  //process_message
			} else {
				statusArgs.add(this.process_message);  //process_message
			}
			statusArgs.add(this.start_process_datetime_flag);  //start_process_datetime
			statusArgs.add(this.end_process_datetime_flag);  //end_process_datetime
			statusArgs.add(this.task_queue_id);  //task_queue_id
			statusArgs.add(this.task_type_code);// task_type_code
			statusArgs.add(this.source_sys_key);  //source_sys_key
			statusArgs.add(this.process_group_id);// process_group_id
			
			// 执行状态更新
			dcETLHelper.setAndExecuteDML(GlobalSql.UPDATE_DP_TASK_QUEUES_STATUS, statusArgs);
			dcETLHelper.commit();
			statusArgs.clear();	
		}
	}
	
}
