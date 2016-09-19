package tables;

import java.util.Date;

public class Transaction_headers {

    private String source_sys_key;
    private String legal_company;
    private String unit_name;
    private int unit_id;
    private int vendor_id;
    private String vendor_number;
    private String vendor_name;
    private String receive_header_id;
    private String receive_num;
    private String receiv_period;
    private Date receive_head_date;
    private double receive_amount;
    private Date dc_creation_date;
    private Date dc_last_update_date;
    
	public String getSource_sys_key() {
		return source_sys_key;
	}
	public void setSource_sys_key(String source_sys_key) {
		this.source_sys_key = source_sys_key;
	}
	public String getLegal_company() {
		return legal_company;
	}
	public void setLegal_company(String legal_company) {
		this.legal_company = legal_company;
	}
	public String getUnit_name() {
		return unit_name;
	}
	public void setUnit_name(String unit_name) {
		this.unit_name = unit_name;
	}
	public int getUnit_id() {
		return unit_id;
	}
	public void setUnit_id(int unit_id) {
		this.unit_id = unit_id;
	}
	public int getVendor_id() {
		return vendor_id;
	}
	public void setVendor_id(int vendor_id) {
		this.vendor_id = vendor_id;
	}
	public String getVendor_number() {
		return vendor_number;
	}
	public void setVendor_number(String vendor_number) {
		this.vendor_number = vendor_number;
	}
	public String getVendor_name() {
		return vendor_name;
	}
	public void setVendor_name(String vendor_name) {
		this.vendor_name = vendor_name;
	}
	public String getReceive_header_id() {
		return receive_header_id;
	}
	public void setReceive_header_id(String receive_header_id) {
		this.receive_header_id = receive_header_id;
	}
	public String getReceive_num() {
		return receive_num;
	}
	public void setReceive_num(String receive_num) {
		this.receive_num = receive_num;
	}
	public String getReceiv_period() {
		return receiv_period;
	}
	public void setReceiv_period(String receiv_period) {
		this.receiv_period = receiv_period;
	}
	public Date getReceive_head_date() {
		return receive_head_date;
	}
	public void setReceive_head_date(Date receive_head_date) {
		this.receive_head_date = receive_head_date;
	}
	public double getReceive_amount() {
		return receive_amount;
	}
	public void setReceive_amount(double receive_amount) {
		this.receive_amount = receive_amount;
	}
	public Date getDc_creation_date() {
		return dc_creation_date;
	}
	public void setDc_creation_date(Date dc_creation_date) {
		this.dc_creation_date = dc_creation_date;
	}
	public Date getDc_last_update_date() {
		return dc_last_update_date;
	}
	public void setDc_last_update_date(Date dc_last_update_date) {
		this.dc_last_update_date = dc_last_update_date;
	}
    
}
