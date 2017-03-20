package entity;

public class QuerySourceTabColRsEO {
	
	private String dp_rules_header_id;  // Added by chao.tang@2017-02-10 for 日志分析
	private String source_sys_key;
	private String source_table_owner;
	private String source_table_name;
	private String dest_table_owner;
	private String dest_table_name;
	private String from_condition_tab_list;
	private String where_condition_sql;
	private String source_tab_column_name;
	private String dp_return_column_val;
	private String dest_tab_column_name;
	private String column_name;
    
    public QuerySourceTabColRsEO() {
    }
    
	public QuerySourceTabColRsEO(String dp_rules_header_id,
			                     String source_sys_key,
								 String source_table_owner, 
								 String source_table_name,
								 String dest_table_owner, 
								 String dest_table_name,
								 String from_condition_tab_list, 
								 String where_condition_sql,
								 String source_tab_column_name, 
								 String dp_return_column_val,
								 String dest_tab_column_name, 
								 String column_name) {
		super();
		this.dp_rules_header_id = dp_rules_header_id;
		this.source_sys_key = source_sys_key;
		this.source_table_owner = source_table_owner;
		this.source_table_name = source_table_name;
		this.dest_table_owner = dest_table_owner;
		this.dest_table_name = dest_table_name;
		this.from_condition_tab_list = from_condition_tab_list;
		this.where_condition_sql = where_condition_sql;
		this.source_tab_column_name = source_tab_column_name;
		this.dp_return_column_val = dp_return_column_val;
		this.dest_tab_column_name = dest_tab_column_name;
		this.column_name = column_name;
	}
	
	public String getDp_rules_header_id() {
		return dp_rules_header_id;
	}

	public void setDp_rules_header_id(String dp_rules_header_id) {
		this.dp_rules_header_id = dp_rules_header_id;
	}

	public String getSource_sys_key() {
		return source_sys_key;
	}
	public void setSource_sys_key(String source_sys_key) {
		this.source_sys_key = source_sys_key;
	}
	public String getSource_table_owner() {
		return source_table_owner;
	}
	public void setSource_table_owner(String source_table_owner) {
		this.source_table_owner = source_table_owner;
	}
	public String getSource_table_name() {
		return source_table_name;
	}
	public void setSource_table_name(String source_table_name) {
		this.source_table_name = source_table_name;
	}
	public String getDest_table_owner() {
		return dest_table_owner;
	}
	public void setDest_table_owner(String dest_table_owner) {
		this.dest_table_owner = dest_table_owner;
	}
	public String getDest_table_name() {
		return dest_table_name;
	}
	public void setDest_table_name(String dest_table_name) {
		this.dest_table_name = dest_table_name;
	}
	public String getFrom_condition_tab_list() {
		return from_condition_tab_list;
	}
	public void setFrom_condition_tab_list(String from_condition_tab_list) {
		this.from_condition_tab_list = from_condition_tab_list;
	}
	public String getWhere_condition_sql() {
		return where_condition_sql;
	}
	public void setWhere_condition_sql(String where_condition_sql) {
		this.where_condition_sql = where_condition_sql;
	}
	public String getSource_tab_column_name() {
		return source_tab_column_name;
	}
	public void setSource_tab_column_name(String source_tab_column_name) {
		this.source_tab_column_name = source_tab_column_name;
	}
	public String getDp_return_column_val() {
		return dp_return_column_val;
	}
	public void setDp_return_column_val(String dp_return_column_val) {
		this.dp_return_column_val = dp_return_column_val;
	}
	public String getDest_tab_column_name() {
		return dest_tab_column_name;
	}
	public void setDest_tab_column_name(String dest_tab_column_name) {
		this.dest_tab_column_name = dest_tab_column_name;
	}
	public String getColumn_name() {
		return column_name;
	}
	public void setColumn_name(String column_name) {
		this.column_name = column_name;
	}

	@Override
	public String toString() {
		return "QuerySourceTabColRsEO [dp_rules_header_id=" + dp_rules_header_id + ", source_sys_key=" + source_sys_key
				+ ", source_table_owner=" + source_table_owner + ", source_table_name=" + source_table_name
				+ ", dest_table_owner=" + dest_table_owner + ", dest_table_name=" + dest_table_name
				+ ", from_condition_tab_list=" + from_condition_tab_list + ", where_condition_sql="
				+ where_condition_sql + ", source_tab_column_name=" + source_tab_column_name + ", dp_return_column_val="
				+ dp_return_column_val + ", dest_tab_column_name=" + dest_tab_column_name + ", column_name="
				+ column_name + "]";
	} 
	
}
