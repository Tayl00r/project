package filter;

import org.mule.api.MuleMessage;
import org.mule.api.routing.filter.Filter;

import config.GlobalInfo;

public class MqMessageFilter implements Filter {

	@Override
	public boolean accept(MuleMessage message) {
		// TODO Auto-generated method stub
		
		try {
			String msg = message.getPayloadAsString();
			if (msg != null && !"".equals(msg) && !GlobalInfo.DC_QUEUES_ERROR_STS.equals(msg)) {
				return true;
			} else {
				return false;
			}
			
		} catch (Exception e) {
			
		}
		
		return false;
	}

}
