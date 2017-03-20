package iface.parameter;

import java.text.ParseException;
import java.util.Date;

import utility.TypeConversionUtil;

public class IfaceParameters {

	private String sourceSysKey;
	private String sourceTabOwner;
	private String sourceTabName;
	private Date dpStartDate;
	private Date dpEndDate;
	private String initialDataImpFlag;
	
	// Constructor 1
	public IfaceParameters(String pSourceSysKey,
						   String pSourceTabOwner,
						   String pSourceTabName,
						   Date pDpStartDate,
						   Date pDpEndDate,
						   String pInitialDataImpFlag) {
		super();
		this.sourceSysKey = pSourceSysKey;
		this.sourceTabOwner = pSourceTabOwner;
		this.sourceTabName = pSourceTabName;
		this.dpStartDate = pDpStartDate;
		this.dpEndDate = pDpEndDate;
		this.initialDataImpFlag = pInitialDataImpFlag;
	}
	
	// Constructor 2
	public IfaceParameters(String pSourceSysKey,
						   String pSourceTabOwner,
						   String pSourceTabName,
						   String pDpStartDate,
						   String pDpEndDate,
						   String pInitialDataImpFlag) {
		super();
		this.sourceSysKey = pSourceSysKey;
		this.sourceTabOwner = pSourceTabOwner;
		this.sourceTabName = pSourceTabName;
		try {
			this.dpStartDate = TypeConversionUtil.dateCheck(pDpStartDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			this.dpStartDate = null;
			e.printStackTrace();
		}
		try {
			this.dpEndDate = TypeConversionUtil.dateCheck(pDpEndDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			this.dpEndDate = null;
			e.printStackTrace();
		}
		this.initialDataImpFlag = pInitialDataImpFlag;
	}
	
	// Constructor 3
	public IfaceParameters(String pSourceSysKey,
						   String pSourceTabOwner,
						   String pSourceTabName) {
		super();
		this.sourceSysKey = pSourceSysKey;
		this.sourceTabOwner = pSourceTabOwner;
		this.sourceTabName = pSourceTabName;
		this.dpStartDate = null;
		this.dpEndDate = null;
		this.initialDataImpFlag = "N";
	}
	
	public String getSourceSysKey() {
		return sourceSysKey;
	}
	public void setSourceSysKey(String sourceSysKey) {
		this.sourceSysKey = sourceSysKey;
	}
	public String getSourceTabOwner() {
		return sourceTabOwner;
	}
	public void setSourceTabOwner(String sourceTabOwner) {
		this.sourceTabOwner = sourceTabOwner;
	}
	public String getSourceTabName() {
		return sourceTabName;
	}
	public void setSourceTabName(String sourceTabName) {
		this.sourceTabName = sourceTabName;
	}
	public Date getDpStartDate() {
		return dpStartDate;
	}
	public void setDpStartDate(Date dpStartDate) {
		this.dpStartDate = dpStartDate;
	}
	public Date getDpEndDate() {
		return dpEndDate;
	}
	public void setDpEndDate(Date dpEndDate) {
		this.dpEndDate = dpEndDate;
	}

	public String getInitialDataImpFlag() {
		return initialDataImpFlag;
	}

	public void setInitialDataImpFlag(String initialDataImpFlag) {
		this.initialDataImpFlag = initialDataImpFlag;
	}
	
}
