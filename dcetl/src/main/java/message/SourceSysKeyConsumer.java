package message;


import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.mule.api.MuleEventContext;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.Callable;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import com.hscf.activemq.ActiveMqConfiger;

import config.GlobalInfo;
import utility.LogManager;
import utility.TypeConversionUtil;

public class SourceSysKeyConsumer implements ExceptionListener, Callable {

	@Override
	public void onException(JMSException arg0) {
		LogManager.appendToLog("SourceSysKeyConsumer Exception occured:" + arg0.toString() );
	}
	
	@Override
	public Object onCall(MuleEventContext eventContext) throws Exception {
        
		LogManager.appendToLog("Entering SourceSysKeyConsumer.onCall: ---> ", GlobalInfo.STATEMENT_MODE);
		
		MuleMessage muleMsg = eventContext.getMessage();
		
		System.setProperty("javax.net.ssl.keyStore", ActiveMqConfiger.keyStore);
		System.setProperty("javax.net.ssl.keyStorePassword",ActiveMqConfiger.keyStorePassword);
		System.setProperty("javax.net.ssl.trustStore",ActiveMqConfiger.trustStore);
		
		//配置Consumer
		ConnectionFactory connectionFactory;
		Connection connection = null;
		Session session;
		Destination destination;
		MessageConsumer consumer;
		
		try {
			LogManager.appendToLog(">Start connect to MQ server by URL: " + ActiveMqConfiger.brokerUrl, 
					GlobalInfo.STATEMENT_MODE);
			
			connectionFactory = new ActiveMQConnectionFactory(ActiveMqConfiger.brokerUrl);
			connection = connectionFactory.createConnection();
			connection.start();
			session = connection.createSession(Boolean.FALSE, Session.CLIENT_ACKNOWLEDGE);
			destination = session.createQueue(GlobalInfo.DC2DCETL_MQ_NAME);
			consumer = session.createConsumer(destination);
			SourceSysKeyListener listener = new SourceSysKeyListener(session);
			consumer.setMessageListener(listener);
			
			LogManager.appendToLog(">Set MessageListener(SourceSysKeyListener) successfully @" + TypeConversionUtil.dateToString(new Date()), 
					GlobalInfo.STATEMENT_MODE);
			
			Thread.sleep(3000000);  // 3000s=50min
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Connect MQ server & set MessageListener(SourceSysKeyListener) error: " + 
					e.toString());
		} finally {
			try {
				if (connection!=null)
					connection.close();
			} catch (Throwable ignore) {
			}
		}
		
		LogManager.appendToLog("Leaving SourceSysKeyConsumer.onCall: ---< ", GlobalInfo.STATEMENT_MODE);
		
		return muleMsg;
	}

}
