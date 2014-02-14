import static org.junit.Assert.*;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;

public class rmqTest {
	
	String mqHost = "localhost";
	String mqPortString = "5672";
	String username = "oo";
	String password = "Test1234";
	String virtualHost = "ooHost";
	String queueName = "junit";
	String exchange = "";

	/* public String getValueFromMap(Map<String, String> map, String value) {
		for (Map.Entry<String, String> entry: map.entrySet()) {
			if (value.equals(entry.getKey())) {
				return entry.getValue();
			}
		}
		return null;
	} */
	
	
	@Test
	public void sendAndRetrieveTest() {
		rmq q = new rmq();
		Map<String, String> aMesg = new HashMap<String, String>();
		Map<String, String> rMesg = new HashMap<String, String>();
		Map<String, String> sMesg = new HashMap<String, String>();
		String dTag = "";
		
		String channelId;
		
		// q.send(message, mqHost, port, user, pass, virtualHost, exchange, queue)
		sMesg = q.send("myMessage", null, "false", mqHost, mqPortString, username, password, virtualHost, exchange, queueName,
				"", "", "", "text/plain", "", "", "", 
				"{\"h1\":13, \"h2\": \"MeinText\", \"h3\":true}", 
				"", "", "", "dd.MM.yyyy", "now", "", "");
		
		channelId = sMesg.get("channelId");
		assertTrue("return message wrong: "+sMesg.get("resultMessage"), sMesg.get("resultMessage").equals("message sent"));
		
		rMesg = q.retrieve(channelId, null, null, null, null, null, null, queueName, null, null);
		//rMesg = q.retrieve(channelId, "false", mqHost, mqPortString, username, password, virtualHost, queueName, "false", "");
		
		assertFalse(rMesg.get("resultMessage"),
				rMesg.get("resultMessage").contains("no message available"));
		
		assertTrue("wrong result message",
				rMesg.get("resultMessage").equals("message retrieved"));
		
		assertTrue("sending or retrieving message failed", 
				rMesg.get("message").equals("myMessage"));
		
		dTag = rMesg.get("deliveryTag");
		
		aMesg = q.ack(channelId, "true", dTag, "false");
		
		assertTrue("message not ack'ed", aMesg.get("resultMessage").equals("message(s) ack'ed"));
			
		rMesg = q.retrieve(channelId, null, null, null, null, null, null, queueName, null, null);
		assertTrue("empty queue not recognized", rMesg.get("resultMessage").equals("no message available"));
	}
	
	
	@Test
	public void createChannelTest()
	{
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		assertTrue("could not get UUID", channelId.length() > 1);
		assertTrue("channel could not be closed", mesg.get("resultMessage").equals("channel created"));
		
		mesg = q.recover(channelId, "", "true");
		assertTrue("channel could not be recovered", mesg.get("resultMessage").equals("channel recovered"));
		
		mesg = q.closeChannel(channelId);
		assertTrue("channel could not be closed", mesg.get("resultMessage").equals("channel closed"));
	}
	
}
