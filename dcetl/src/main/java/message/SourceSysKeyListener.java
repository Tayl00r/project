package message;

import java.util.Date;
import java.util.Enumeration;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;

import utility.LogManager;
import utility.MqCallDataTransform;
import utility.TypeConversionUtil;


public class SourceSysKeyListener implements MessageListener {

	private Session session;

	public SourceSysKeyListener(Session session) {
		this.session = session;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(Message arg0) {

		try {
			// 获得all2dc传输过来的SourceSysKey
			MapMessage mapMessage = (MapMessage) arg0;
			Enumeration<Object> maps = mapMessage.getMapNames();
			String all2dcMessage = new String();
			while (maps.hasMoreElements()) {
				String key = (String) maps.nextElement();
				all2dcMessage = (String) mapMessage.getObject(key);
			}
			
			if (all2dcMessage != null && !"".equals(all2dcMessage)) {
				LogManager.appendToLog(">>Listening SourceSysKeyListener.onMessage() got the all2dcMessage: " + all2dcMessage + " @" + 
						TypeConversionUtil.dateToString(new Date()) + "<<");
				
				MqCallDataTransform mqCallDataTransform = new MqCallDataTransform();
				mqCallDataTransform.startDataTransform(all2dcMessage);
			}
			
			arg0.acknowledge();
			
			Thread.sleep(30000);  // 休眠30s

		} catch (Exception finalException) {

			try {
				session.recover();
			} catch (JMSException jmsException) {
				jmsException.printStackTrace();
			}

			finalException.printStackTrace();
			
			LogManager.appendToLog("Listening SourceSysKeyListener.onMessage() Exception: " + finalException.toString() + " @" + 
					TypeConversionUtil.dateToString(new Date()) );
			
		}

	}

}
