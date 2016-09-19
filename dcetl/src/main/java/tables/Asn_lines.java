package tables;

import java.util.Date;

public class Asn_lines {

    private String source_sys_key;
    private int asn_header_id;
    private String order_header_id;
    private String order_number;
    private String order_line_id;
    private String line_num;
    private int location_line_id;
    private String location_line_num;
    private int asn_line_id;
    private String asn_line_number;
    private double order_quality;
    private double receive_quality;
    private double deliver_quality;
    private double shipment_quality;
    private String ship_to_location;
    private int item_id;
    private String item_number;
    private String item_description;
    private double unit_price;
    private String unit;
    private Date due_date;
    private String status;
    private String cancel_status;
    private String receive_status;
    private String deliver_status;
    private String lpn_lots_number;
    private double return_to_receive;
    private double return_to_vendor;
    private double cancel_quality;
    private double close_quality;
    private String receive_number;
    private Date receive_date;
    private Date cancel_date;
    private String cancel_person;
    private String cancel_reason;
    private String close_flag;
    private Date close_date;
    private String close_person;
    private String close_reason;
    private Date promise_date;
    private int warehouse_id;
    private String warehouse_description;
    private Date dc_creation_date;
    private Date dc_last_update_date;
    
	public String getSource_sys_key() {
		return source_sys_key;
	}
	public void setSource_sys_key(String source_sys_key) {
		this.source_sys_key = source_sys_key;
	}
	public int getAsn_header_id() {
		return asn_header_id;
	}
	public void setAsn_header_id(int asn_header_id) {
		this.asn_header_id = asn_header_id;
	}
	public String getOrder_header_id() {
		return order_header_id;
	}
	public void setOrder_header_id(String order_header_id) {
		this.order_header_id = order_header_id;
	}
	public String getOrder_number() {
		return order_number;
	}
	public void setOrder_number(String order_number) {
		this.order_number = order_number;
	}
	public String getOrder_line_id() {
		return order_line_id;
	}
	public void setOrder_line_id(String order_line_id) {
		this.order_line_id = order_line_id;
	}
	public String getLine_num() {
		return line_num;
	}
	public void setLine_num(String line_num) {
		this.line_num = line_num;
	}
	public int getLocation_line_id() {
		return location_line_id;
	}
	public void setLocation_line_id(int location_line_id) {
		this.location_line_id = location_line_id;
	}
	public String getLocation_line_num() {
		return location_line_num;
	}
	public void setLocation_line_num(String location_line_num) {
		this.location_line_num = location_line_num;
	}
	public int getAsn_line_id() {
		return asn_line_id;
	}
	public void setAsn_line_id(int asn_line_id) {
		this.asn_line_id = asn_line_id;
	}
	public String getAsn_line_number() {
		return asn_line_number;
	}
	public void setAsn_line_number(String asn_line_number) {
		this.asn_line_number = asn_line_number;
	}
	public double getOrder_quality() {
		return order_quality;
	}
	public void setOrder_quality(double order_quality) {
		this.order_quality = order_quality;
	}
	public double getReceive_quality() {
		return receive_quality;
	}
	public void setReceive_quality(double receive_quality) {
		this.receive_quality = receive_quality;
	}
	public double getDeliver_quality() {
		return deliver_quality;
	}
	public void setDeliver_quality(double deliver_quality) {
		this.deliver_quality = deliver_quality;
	}
	public double getShipment_quality() {
		return shipment_quality;
	}
	public void setShipment_quality(double shipment_quality) {
		this.shipment_quality = shipment_quality;
	}
	public String getShip_to_location() {
		return ship_to_location;
	}
	public void setShip_to_location(String ship_to_location) {
		this.ship_to_location = ship_to_location;
	}
	public int getItem_id() {
		return item_id;
	}
	public void setItem_id(int item_id) {
		this.item_id = item_id;
	}
	public String getItem_number() {
		return item_number;
	}
	public void setItem_number(String item_number) {
		this.item_number = item_number;
	}
	public String getItem_description() {
		return item_description;
	}
	public void setItem_description(String item_description) {
		this.item_description = item_description;
	}
	public double getUnit_price() {
		return unit_price;
	}
	public void setUnit_price(double unit_price) {
		this.unit_price = unit_price;
	}
	public String getUnit() {
		return unit;
	}
	public void setUnit(String unit) {
		this.unit = unit;
	}
	public Date getDue_date() {
		return due_date;
	}
	public void setDue_date(Date due_date) {
		this.due_date = due_date;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getCancel_status() {
		return cancel_status;
	}
	public void setCancel_status(String cancel_status) {
		this.cancel_status = cancel_status;
	}
	public String getReceive_status() {
		return receive_status;
	}
	public void setReceive_status(String receive_status) {
		this.receive_status = receive_status;
	}
	public String getDeliver_status() {
		return deliver_status;
	}
	public void setDeliver_status(String deliver_status) {
		this.deliver_status = deliver_status;
	}
	public String getLpn_lots_number() {
		return lpn_lots_number;
	}
	public void setLpn_lots_number(String lpn_lots_number) {
		this.lpn_lots_number = lpn_lots_number;
	}
	public double getReturn_to_receive() {
		return return_to_receive;
	}
	public void setReturn_to_receive(double return_to_receive) {
		this.return_to_receive = return_to_receive;
	}
	public double getReturn_to_vendor() {
		return return_to_vendor;
	}
	public void setReturn_to_vendor(double return_to_vendor) {
		this.return_to_vendor = return_to_vendor;
	}
	public double getCancel_quality() {
		return cancel_quality;
	}
	public void setCancel_quality(double cancel_quality) {
		this.cancel_quality = cancel_quality;
	}
	public double getClose_quality() {
		return close_quality;
	}
	public void setClose_quality(double close_quality) {
		this.close_quality = close_quality;
	}
	public String getReceive_number() {
		return receive_number;
	}
	public void setReceive_number(String receive_number) {
		this.receive_number = receive_number;
	}
	public Date getReceive_date() {
		return receive_date;
	}
	public void setReceive_date(Date receive_date) {
		this.receive_date = receive_date;
	}
	public Date getCancel_date() {
		return cancel_date;
	}
	public void setCancel_date(Date cancel_date) {
		this.cancel_date = cancel_date;
	}
	public String getCancel_person() {
		return cancel_person;
	}
	public void setCancel_person(String cancel_person) {
		this.cancel_person = cancel_person;
	}
	public String getCancel_reason() {
		return cancel_reason;
	}
	public void setCancel_reason(String cancel_reason) {
		this.cancel_reason = cancel_reason;
	}
	public String getClose_flag() {
		return close_flag;
	}
	public void setClose_flag(String close_flag) {
		this.close_flag = close_flag;
	}
	public Date getClose_date() {
		return close_date;
	}
	public void setClose_date(Date close_date) {
		this.close_date = close_date;
	}
	public String getClose_person() {
		return close_person;
	}
	public void setClose_person(String close_person) {
		this.close_person = close_person;
	}
	public String getClose_reason() {
		return close_reason;
	}
	public void setClose_reason(String close_reason) {
		this.close_reason = close_reason;
	}
	public Date getPromise_date() {
		return promise_date;
	}
	public void setPromise_date(Date promise_date) {
		this.promise_date = promise_date;
	}
	public int getWarehouse_id() {
		return warehouse_id;
	}
	public void setWarehouse_id(int warehouse_id) {
		this.warehouse_id = warehouse_id;
	}
	public String getWarehouse_description() {
		return warehouse_description;
	}
	public void setWarehouse_description(String warehouse_description) {
		this.warehouse_description = warehouse_description;
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
