package tables;

import java.util.Date;

public class Payment_lines {

    private String source_sys_key;
    private String legal_company;
    private String unit_name;
    private int unit_id;
    private int vendor_id;
    private String vendor_number;
    private String vendor_name;
    private String pay_period;
    private Date pay_date;
    private String check_id;
    private String check_line_id;
    private String pay_number;
    private String pay_line_num;
    private String invoice_number;
    private double payment_amount;
    private Date invoice_gl_date;
    private String invoice_head_id;
    private double invoice_amount;
    private Date dc_creation_date;
    private Date dc_last_update_date;
    private double discount_taken;
    private Date creation_date;
    
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
	public String getPay_period() {
		return pay_period;
	}
	public void setPay_period(String pay_period) {
		this.pay_period = pay_period;
	}
	public Date getPay_date() {
		return pay_date;
	}
	public void setPay_date(Date pay_date) {
		this.pay_date = pay_date;
	}
	public String getCheck_id() {
		return check_id;
	}
	public void setCheck_id(String check_id) {
		this.check_id = check_id;
	}
	public String getCheck_line_id() {
		return check_line_id;
	}
	public void setCheck_line_id(String check_line_id) {
		this.check_line_id = check_line_id;
	}
	public String getPay_number() {
		return pay_number;
	}
	public void setPay_number(String pay_number) {
		this.pay_number = pay_number;
	}
	public String getPay_line_num() {
		return pay_line_num;
	}
	public void setPay_line_num(String pay_line_num) {
		this.pay_line_num = pay_line_num;
	}
	public String getInvoice_number() {
		return invoice_number;
	}
	public void setInvoice_number(String invoice_number) {
		this.invoice_number = invoice_number;
	}
	public double getPayment_amount() {
		return payment_amount;
	}
	public void setPayment_amount(double payment_amount) {
		this.payment_amount = payment_amount;
	}
	public Date getInvoice_gl_date() {
		return invoice_gl_date;
	}
	public void setInvoice_gl_date(Date invoice_gl_date) {
		this.invoice_gl_date = invoice_gl_date;
	}
	public String getInvoice_head_id() {
		return invoice_head_id;
	}
	public void setInvoice_head_id(String invoice_head_id) {
		this.invoice_head_id = invoice_head_id;
	}
	public double getInvoice_amount() {
		return invoice_amount;
	}
	public void setInvoice_amount(double invoice_amount) {
		this.invoice_amount = invoice_amount;
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
	public double getDiscount_taken() {
		return discount_taken;
	}
	public void setDiscount_taken(double discount_taken) {
		this.discount_taken = discount_taken;
	}
	public Date getCreation_date() {
		return creation_date;
	}
	public void setCreation_date(Date creation_date) {
		this.creation_date = creation_date;
	}
    
}
