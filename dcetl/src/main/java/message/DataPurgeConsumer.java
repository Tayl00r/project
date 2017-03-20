package message;

import java.util.Date;

import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

import com.hscf.activemq.ActiveMqConfiger;

import utility.LogManager;
import utility.MqCallDataTransform;
import utility.TypeConversionUtil;

public class DataPurgeConsumer implements Callable {

	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		// TODO Auto-generated method stub
		
		LogManager.appendToLog("Entering DataPurgeConsumer.onCall() ---> ");
		
		//获取MQ监听到的消息
		MuleMessage muleMsg = eventContext.getMessage();
		String all2dcMessage = muleMsg.getPayloadAsString();
		
		LogManager.appendToLog(">>Message Listener got the all2dcMessage: " + all2dcMessage + " @" + 
				TypeConversionUtil.dateToString(new Date()) + "<<");
		
		if (all2dcMessage != null && !"".equals(all2dcMessage)) {
			MqCallDataTransform mqCallDataTransform = new MqCallDataTransform();
			mqCallDataTransform.startDataTransform(all2dcMessage);
		}
		
		LogManager.appendToLog("Leaving DataPurgeConsumer.onCall() ---> ");
		
		return muleMsg;
	}
	
}
