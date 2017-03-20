package entity;

public class DcSysDbConnMapping {

	private String source_sys_key;
	private String db_host;
	private String db_port;
	private String db_schema;
	private String db_conn_user;
	private String db_conn_password;
	private String db_url;  // 动态拼接而成

	public String getSource_sys_key() {
		return source_sys_key;
	}

	public void setSource_sys_key(String source_sys_key) {
		this.source_sys_key = source_sys_key;
	}

	public String getDb_host() {
		return db_host;
	}

	public void setDb_host(String db_host) {
		this.db_host = db_host;
	}

	public String getDb_port() {
		return db_port;
	}

	public void setDb_port(String db_port) {
		this.db_port = db_port;
	}

	public String getDb_schema() {
		return db_schema;
	}

	public void setDb_schema(String db_schema) {
		this.db_schema = db_schema;
	}

	public String getDb_conn_user() {
		return db_conn_user;
	}

	public void setDb_conn_user(String db_conn_user) {
		this.db_conn_user = db_conn_user;
	}

	public String getDb_conn_password() {
		return db_conn_password;
	}

	public void setDb_conn_password(String db_conn_password) {
		this.db_conn_password = db_conn_password;
	}

	public String getDb_url() {
		return db_url;
	}

	public void setDb_url(String db_url) {
		this.db_url = db_url;
	}

	@Override
	public String toString() {
		return "DcSysDbConnMapping [source_sys_key=" + source_sys_key + ", db_host=" + db_host + ", db_port=" + db_port
				+ ", db_schema=" + db_schema + ", db_conn_user=" + db_conn_user + ", db_conn_password="
				+ db_conn_password + ", db_url=" + db_url + "]";
	}

}
