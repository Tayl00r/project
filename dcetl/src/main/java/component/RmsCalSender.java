package component;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;

import com.hscf.activemq.ActiveMqConfiger;

import config.GlobalInfo;
import utility.GeneratorUUID;
import utility.LogManager;

public class RmsCalSender implements Callable{
	
	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
		
		LogManager.appendToLog("Entering RmsCalSender.onCall() ---> ");
		
		MuleMessage muleMsg = eventContext.getMessage();
		String message = (String) muleMsg.getPayload();
		
		LogManager.appendToLog("Waiting to send message: " + message);
		
		if (message != null && !"".equals(message)) {
			startSendMessage(message);  
		}
			
		LogManager.appendToLog("Leaving Rms&DwCalSender.onCall() ---> ");

		return muleMsg;
	}
	
	private void startSendMessage(String message) {
		
		LogManager.appendToLog("Entering Rms&DwCalSender.startSendMessage() --->", GlobalInfo.STATEMENT_MODE);
		
		//System.setProperty("javax.net.ssl.keyStore", ActiveMqConfiger.keyStore);
		//System.setProperty("javax.net.ssl.keyStorePassword", ActiveMqConfiger.keyStorePassword);
		//System.setProperty("javax.net.ssl.trustStore", ActiveMqConfiger.trustStore);
		
		ConnectionFactory connectionFactory;
		Connection connection = null;
		Session session;
		Destination destination;
		MessageProducer producer;
		boolean exceptionFlag = false;
		String exceptionMessage = null;
		
		try {
			connectionFactory = new ActiveMQConnectionFactory(ActiveMqConfiger.brokerUrl);
			LogManager.appendToLog(">Rms&DwCalSender.startSendMessage.ActiveMqConfiger.brokerUrl: " + 
					ActiveMqConfiger.brokerUrl);
			
			connection = connectionFactory.createConnection();
			connection.start();
			// 发送消息给RMS
			session = connection.createSession(Boolean.TRUE,Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue(GlobalInfo.DCETL2RMS_MQ_NAME);
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			sendMessage(session, producer, message , GlobalInfo.DCETL2RMS_MQ_NAME);
			session.commit();
			LogManager.appendToLog(">RmsCalSender.startSendMessage() ---> SUCCEED");
			
			// 发送消息给DW
			session = connection.createSession(Boolean.TRUE,Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue(GlobalInfo.DCETL2DW_MQ_NAME);
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			sendMessage(session, producer, message , GlobalInfo.DCETL2DW_MQ_NAME);
			session.commit();
			LogManager.appendToLog(">DwCalSender.startSendMessage() ---> SUCCEED");
			
		} catch (Exception e) {
			e.printStackTrace();
			exceptionFlag = true;
			exceptionMessage = ">Rms&DwCalSender.startSendMessage() ---> FAILED" + GlobalInfo.LINE_SEPARATOR + e.toString();
			LogManager.appendToLog(exceptionMessage);
		} finally {
			try {
				if (null != connection)
					connection.close();
			} catch (Throwable ignore) {
			}
		}
		
		// 判断是否抛出异常，邮件通知 Added by chao.tang@2016-10-11 14:14:14 Begin
		if (exceptionFlag) {
			throw new RuntimeException(exceptionMessage);
		}
		// 判断是否抛出异常，邮件通知 Added by chao.tang@2016-10-11 14:14:14 End
		
		LogManager.appendToLog("Leaving Rms&DwCalSender.startSendMessage() --->", GlobalInfo.STATEMENT_MODE);
	}
	
	public static void sendMessage(Session session, MessageProducer producer, String message, String msgType) throws Exception {
		
		String[] message0 = message.split("\\:");
		
		MapMessage mapMsg = session.createMapMessage();
		// String message="highly:b921f114635811e6ac18177663a447a7:rms_cal_process_group_id";
		if (msgType.equals(GlobalInfo.DCETL2RMS_MQ_NAME)) {
			Map<String, String> msgContent = new HashMap<String, String>();
			msgContent.put("batchNum", GeneratorUUID.generateUUID());
			msgContent.put("event", GlobalInfo.DC2RMS_DCDATAENTRY);
			msgContent.put("sourceSysKey", message0[0]);
			msgContent.put("processGroupId", message0[1]);
			msgContent.put("activityNodeId", message0[2]);
			mapMsg.setObject("realtimeCalculate", msgContent);
			LogManager.appendToLog("RMS's MQ : " + GlobalInfo.DCETL2RMS_MQ_NAME + mapMsg.toString());
			producer.send(mapMsg);
		}

		if (msgType.equals(GlobalInfo.DCETL2DW_MQ_NAME)) {
			Map<String, String> msgContent = new HashMap<String, String>();
			msgContent.put("calcEventType", null);
			msgContent.put("sourceSysKey", message0[0]);
			msgContent.put("processGroupId", message0[1]);
			msgContent.put("activityNodeId", message0[2]);
			msgContent.put("sourceCode", GlobalInfo.DC_QUEUES_TASK_TYPE);
			mapMsg.setObject("dwStbMonitor", msgContent);
			LogManager.appendToLog("DW's MQ : " + GlobalInfo.DCETL2DW_MQ_NAME + mapMsg.toString());
			producer.send(mapMsg);
		}

	}

}

