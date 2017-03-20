package entity;

import java.util.Date;

public class QueryDPInstanceConfigsRsEO {
	
	private String dp_inst_config_id;
	private String source_dp_table_owner;
	private String source_dp_table_name;
	private String source_dp_tab_filter_condit1;
	private String source_dp_tab_filter_condit2;
	private String source_dp_tab_filter_condit3;
	private String source_dp_tab_filter_condit4;
	private String source_dp_tab_filter_condit5;
	private String dest_dp_table_owner;
	private String dest_dp_table_name;
	private int do_dp_per_seconds;
	private int dp_tolerance_seconds;
	private Date last_dp_datetime;
	private String dp_inst_config_autoc_id;
	private String source_sys_key;
	private String dp_rules_header_id;  // 规则头ID
    
    public QueryDPInstanceConfigsRsEO() {
    }
    
	public QueryDPInstanceConfigsRsEO(String dp_inst_config_id,
			                          String source_dp_table_owner,
									  String source_dp_table_name, 
									  String source_dp_tab_filter_condit1,
									  String source_dp_tab_filter_condit2,
									  String source_dp_tab_filter_condit3,
									  String source_dp_tab_filter_condit4,
									  String source_dp_tab_filter_condit5, 
									  String dest_dp_table_owner,
									  String dest_dp_table_name,
									  int do_dp_per_seconds,
									  int dp_tolerance_seconds,
									  Date last_dp_datetime,
									  String dp_inst_config_autoc_id,
									  String source_sys_key,
									  String dp_rules_header_id) {
		super();
		this.dp_inst_config_id = dp_inst_config_id;
		this.source_dp_table_owner = source_dp_table_owner;
		this.source_dp_table_name = source_dp_table_name;
		this.source_dp_tab_filter_condit1 = source_dp_tab_filter_condit1;
		this.source_dp_tab_filter_condit2 = source_dp_tab_filter_condit2;
		this.source_dp_tab_filter_condit3 = source_dp_tab_filter_condit3;
		this.source_dp_tab_filter_condit4 = source_dp_tab_filter_condit4;
		this.source_dp_tab_filter_condit5 = source_dp_tab_filter_condit5;
		this.dest_dp_table_owner = dest_dp_table_owner;
		this.dest_dp_table_name = dest_dp_table_name;
		this.do_dp_per_seconds = do_dp_per_seconds;
		this.dp_tolerance_seconds = dp_tolerance_seconds;
		this.last_dp_datetime = last_dp_datetime;
		this.dp_inst_config_autoc_id = dp_inst_config_autoc_id;
		this.source_sys_key = source_sys_key;
		this.dp_rules_header_id = dp_rules_header_id;
	}
	
	public String getDp_inst_config_id() {
		return dp_inst_config_id;
	}

	public void setDp_inst_config_id(String dp_inst_config_id) {
		this.dp_inst_config_id = dp_inst_config_id;
	}

	public String getSource_dp_table_owner() {
		return source_dp_table_owner;
	}
	public void setSource_dp_table_owner(String source_dp_table_owner) {
		this.source_dp_table_owner = source_dp_table_owner;
	}
	public String getSource_dp_table_name() {
		return source_dp_table_name;
	}
	public void setSource_dp_table_name(String source_dp_table_name) {
		this.source_dp_table_name = source_dp_table_name;
	}
	public String getSource_dp_tab_filter_condit1() {
		return source_dp_tab_filter_condit1;
	}
	public void setSource_dp_tab_filter_condit1(String source_dp_tab_filter_condit1) {
		this.source_dp_tab_filter_condit1 = source_dp_tab_filter_condit1;
	}
	public String getSource_dp_tab_filter_condit2() {
		return source_dp_tab_filter_condit2;
	}
	public void setSource_dp_tab_filter_condit2(String source_dp_tab_filter_condit2) {
		this.source_dp_tab_filter_condit2 = source_dp_tab_filter_condit2;
	}
	public String getSource_dp_tab_filter_condit3() {
		return source_dp_tab_filter_condit3;
	}
	public void setSource_dp_tab_filter_condit3(String source_dp_tab_filter_condit3) {
		this.source_dp_tab_filter_condit3 = source_dp_tab_filter_condit3;
	}
	public String getSource_dp_tab_filter_condit4() {
		return source_dp_tab_filter_condit4;
	}
	public void setSource_dp_tab_filter_condit4(String source_dp_tab_filter_condit4) {
		this.source_dp_tab_filter_condit4 = source_dp_tab_filter_condit4;
	}
	public String getSource_dp_tab_filter_condit5() {
		return source_dp_tab_filter_condit5;
	}
	public void setSource_dp_tab_filter_condit5(String source_dp_tab_filter_condit5) {
		this.source_dp_tab_filter_condit5 = source_dp_tab_filter_condit5;
	}
	public String getDest_dp_table_owner() {
		return dest_dp_table_owner;
	}
	public void setDest_dp_table_owner(String dest_dp_table_owner) {
		this.dest_dp_table_owner = dest_dp_table_owner;
	}
	public String getDest_dp_table_name() {
		return dest_dp_table_name;
	}
	public void setDest_dp_table_name(String dest_dp_table_name) {
		this.dest_dp_table_name = dest_dp_table_name;
	}

	public int getDo_dp_per_seconds() {
		return do_dp_per_seconds;
	}

	public void setDo_dp_per_seconds(int do_dp_per_seconds) {
		this.do_dp_per_seconds = do_dp_per_seconds;
	}

	public int getDp_tolerance_seconds() {
		return dp_tolerance_seconds;
	}

	public void setDp_tolerance_seconds(int dp_tolerance_seconds) {
		this.dp_tolerance_seconds = dp_tolerance_seconds;
	}

	public Date getLast_dp_datetime() {
		return last_dp_datetime;
	}

	public void setLast_dp_datetime(Date last_dp_datetime) {
		this.last_dp_datetime = last_dp_datetime;
	}

	public String getDp_inst_config_autoc_id() {
		return dp_inst_config_autoc_id;
	}

	public void setDp_inst_config_autoc_id(String dp_inst_config_autoc_id) {
		this.dp_inst_config_autoc_id = dp_inst_config_autoc_id;
	}

	public String getSource_sys_key() {
		return source_sys_key;
	}

	public void setSource_sys_key(String source_sys_key) {
		this.source_sys_key = source_sys_key;
	}

	public String getDp_rules_header_id() {
		return dp_rules_header_id;
	}

	public void setDp_rules_header_id(String dp_rules_header_id) {
		this.dp_rules_header_id = dp_rules_header_id;
	}

	@Override
	public String toString() {
		return "QueryDPInstanceConfigsRsEO [dp_inst_config_id=" + dp_inst_config_id + ", source_dp_table_owner="
				+ source_dp_table_owner + ", source_dp_table_name=" + source_dp_table_name
				+ ", source_dp_tab_filter_condit1=" + source_dp_tab_filter_condit1 + ", source_dp_tab_filter_condit2="
				+ source_dp_tab_filter_condit2 + ", source_dp_tab_filter_condit3=" + source_dp_tab_filter_condit3
				+ ", source_dp_tab_filter_condit4=" + source_dp_tab_filter_condit4 + ", source_dp_tab_filter_condit5="
				+ source_dp_tab_filter_condit5 + ", dest_dp_table_owner=" + dest_dp_table_owner
				+ ", dest_dp_table_name=" + dest_dp_table_name + ", do_dp_per_seconds=" + do_dp_per_seconds
				+ ", dp_tolerance_seconds=" + dp_tolerance_seconds + ", last_dp_datetime=" + last_dp_datetime
				+ ", dp_inst_config_autoc_id=" + dp_inst_config_autoc_id + ", source_sys_key=" + source_sys_key 
				+ ", dp_rules_header_id=" + dp_rules_header_id + "]";
	}
    
}
