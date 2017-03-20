package dcetl;

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

import com.hscf.activemq.ActiveMqConfiger;

import config.GlobalInfo;
import utility.GeneratorUUID;
import utility.LogManager;

public class Test3 {
    
private static void startSendMessage(String message) {
		
		LogManager.appendToLog("Entering RmsCalSender.startSendMessage() --->", GlobalInfo.STATEMENT_MODE);
		
		System.setProperty("javax.net.ssl.keyStore", ActiveMqConfiger.keyStore);
		System.setProperty("javax.net.ssl.keyStorePassword", ActiveMqConfiger.keyStorePassword);
		System.setProperty("javax.net.ssl.trustStore", ActiveMqConfiger.trustStore);
		
		ConnectionFactory connectionFactory;
		Connection connection = null;
		Session session;
		Destination destination;
		MessageProducer producer;
		
		try {
			connectionFactory = new ActiveMQConnectionFactory(ActiveMqConfiger.brokerUrl);
			LogManager.appendToLog(">RmsCalSender.startSendMessage.ActiveMqConfiger.brokerUrl: " + 
					ActiveMqConfiger.brokerUrl);
			
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(Boolean.TRUE,Session.AUTO_ACKNOWLEDGE);
			destination = session.createQueue("CCU-RMSBACKEND-REALTIME-CALC-TEST");
			producer = session.createProducer(destination);
			producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
			sendMessage(session, producer, message);
			session.commit();
			
			LogManager.appendToLog(">RmsCalSender.startSendMessage() ---> SUCCEED");
			
		} catch (Exception e) {
			e.printStackTrace();
			LogManager.appendToLog(">RmsCalSender.startSendMessage() ---> FAILED" + GlobalInfo.LINE_SEPARATOR + 
					e.toString());
		} finally {
			try {
				if (null != connection)
					connection.close();
			} catch (Throwable ignore) {
			}
		}
		
		LogManager.appendToLog("Leaving RmsCalSender.startSendMessage() --->", GlobalInfo.STATEMENT_MODE);
	}
	
	private static void sendMessage(Session session, MessageProducer producer, String message) throws Exception {
		
		String[] message0 = message.split("\\:");
		
		MapMessage mapMsg = session.createMapMessage();
		//String message="highly:b921f114635811e6ac18177663a447a7:rms_cal_process_group_id";
		
		Map<String, String> msgContent = new HashMap<String, String>();
		msgContent.put("batchNum", GeneratorUUID.generateUUID());
		msgContent.put("event", "DCDATAENTRY");
		msgContent.put("sourceSysKey", message0[0]);
		msgContent.put("processGroupId", message0[1]);
		mapMsg.setObject("realtimeCalculate", msgContent);
		
		producer.send(mapMsg);

	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		startSendMessage("highly:7346cdc6854011e6840e0167fb3bcaac");

	}

}
