package iface.parameter;

import config.GlobalInfo;

public class IfaceReturns {
	
	private String returnStatus;
	private String returnMsg;
	
	public IfaceReturns() {
		super();
		this.returnStatus = GlobalInfo.SERVICES_RET_SUCCESS;
		this.returnMsg = "";
	}
	
	public IfaceReturns(String returnStatus, String returnMsg) {
		super();
		this.returnStatus = returnStatus;
		this.returnMsg = returnMsg;
	}
	
	public String getReturnStatus() {
		return returnStatus;
	}
	public void setReturnStatus(String returnStatus) {
		this.returnStatus = returnStatus;
	}
	public String getReturnMsg() {
		return returnMsg;
	}
	public void setReturnMsg(String returnMsg) {
		this.returnMsg = returnMsg;
	}
	
	
	
}
