package entity;

import java.io.Serializable;

//头表：dc_process_task_queues
public class DcProcessTaskQueues implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String task_queue_id;
	private String task_type_code;
	private String source_sys_key;
	private String process_group_id;
	private String status_code;
	private String process_message;
	private String source_ref_doc_id;
	
	// 行
	DcProcessTaskQueueDetails dcProcessTaskQueueDetails;
	
	public DcProcessTaskQueues() {
		// TODO Auto-generated constructor stub
	}
	
	public DcProcessTaskQueues(DcProcessTaskQueues e) {
		// TODO Auto-generated constructor stub
		this.task_queue_id = e.getTask_queue_id();
		this.task_type_code = e.getTask_type_code();
		this.source_sys_key = e.getSource_sys_key();
		this.process_group_id = e.getProcess_group_id();
		this.status_code = e.getStatus_code();
		this.process_message = e.getProcess_message();
		this.source_ref_doc_id = e.getSource_ref_doc_id();
		this.dcProcessTaskQueueDetails = e.getDcProcessTaskQueueDetails();
	}

	public String getTask_queue_id() {
		return task_queue_id;
	}

	public void setTask_queue_id(String task_queue_id) {
		this.task_queue_id = task_queue_id;
	}

	public String getTask_type_code() {
		return task_type_code;
	}

	public void setTask_type_code(String task_type_code) {
		this.task_type_code = task_type_code;
	}

	public String getSource_sys_key() {
		return source_sys_key;
	}

	public void setSource_sys_key(String source_sys_key) {
		this.source_sys_key = source_sys_key;
	}

	public String getProcess_group_id() {
		return process_group_id;
	}

	public void setProcess_group_id(String process_group_id) {
		this.process_group_id = process_group_id;
	}

	public String getStatus_code() {
		return status_code;
	}

	public void setStatus_code(String status_code) {
		this.status_code = status_code;
	}

	public String getProcess_message() {
		return process_message;
	}

	public void setProcess_message(String process_message) {
		this.process_message = process_message;
	}

	public String getSource_ref_doc_id() {
		return source_ref_doc_id;
	}

	public void setSource_ref_doc_id(String source_ref_doc_id) {
		this.source_ref_doc_id = source_ref_doc_id;
	}

	public DcProcessTaskQueueDetails getDcProcessTaskQueueDetails() {
		return dcProcessTaskQueueDetails;
	}

	public void setDcProcessTaskQueueDetails(DcProcessTaskQueueDetails dcProcessTaskQueueDetails) {
		this.dcProcessTaskQueueDetails = dcProcessTaskQueueDetails;
	}

	@Override
	public String toString() {
		return "DcProcessTaskQueues [task_queue_id=" + task_queue_id + ", task_type_code=" + task_type_code
				+ ", source_sys_key=" + source_sys_key + ", process_group_id=" + process_group_id + ", status_code="
				+ status_code + ", process_message=" + process_message + ", source_ref_doc_id=" + source_ref_doc_id
				+ ", dcProcessTaskQueueDetails=" + dcProcessTaskQueueDetails.toString() + "]";
	}

}
