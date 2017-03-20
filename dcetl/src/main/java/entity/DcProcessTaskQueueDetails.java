package entity;

import java.io.Serializable;

//行表：dc_process_task_queue_details
public class DcProcessTaskQueueDetails implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String process_group_id;
	private String type;
	private String value1;
	private String value2;
	private String value3;
	private String value4;
	private String value5;
	private String process_flag;
	private String process_message;
	
	public String getProcess_group_id() {
		return process_group_id;
	}
	public void setProcess_group_id(String process_group_id) {
		this.process_group_id = process_group_id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getValue1() {
		return value1;
	}
	public void setValue1(String value1) {
		this.value1 = value1;
	}
	public String getValue2() {
		return value2;
	}
	public void setValue2(String value2) {
		this.value2 = value2;
	}
	public String getValue3() {
		return value3;
	}
	public void setValue3(String value3) {
		this.value3 = value3;
	}
	public String getValue4() {
		return value4;
	}
	public void setValue4(String value4) {
		this.value4 = value4;
	}
	public String getValue5() {
		return value5;
	}
	public void setValue5(String value5) {
		this.value5 = value5;
	}
	public String getProcess_flag() {
		return process_flag;
	}
	public void setProcess_flag(String process_flag) {
		this.process_flag = process_flag;
	}
	public String getProcess_message() {
		return process_message;
	}
	public void setProcess_message(String process_message) {
		this.process_message = process_message;
	}
	
	@Override
	public String toString() {
		return "DcProcessTaskQueueDetails [process_group_id=" + process_group_id + ", type=" + type + ", value1="
				+ value1 + ", value2=" + value2 + ", value3=" + value3 + ", value4=" + value4 + ", value5=" + value5
				+ ", process_flag=" + process_flag + ", process_message=" + process_message + "]";
	}
	
}
