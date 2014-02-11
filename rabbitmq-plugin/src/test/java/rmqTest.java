import static org.junit.Assert.*;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;


public class rmqTest {

	public String getValueFromMap(Map<String, String> map, String value) {
		for (Map.Entry<String, String> entry: map.entrySet()) {
			if (value.equals(entry.getKey())) {
				return entry.getValue();
			}
		}
		return null;
	}
	
	
	@Test
	public void test() {
		rmq q = new rmq();
		Map<String, String> rMesg = new HashMap<String, String>();
		Map<String, String> sMesg = new HashMap<String, String>();
		
		// q.send(message, mqHost, port, user, pass, virtualHost, exchange, queue)
		sMesg = q.send("myMessage", "127.0.0.1", "5672", "oo", "Test1234", "ooHost", "amq.rabbitmq.trace", "junit",
				"", "", "", "text/plain", "", "", "", "", "", "", "", "dd.MM.yyyy", "", "", "");
		
		assertTrue("return message wrong", 
				getValueFromMap(sMesg, "resultMessage").equals("message sent"));
				
		rMesg = q.retrieve("127.0.0.1", "5672", "oo", "Test1234", "ooHost", "junit", "true", "");
		
		assertFalse(getValueFromMap(rMesg, "resultMessage"),
				getValueFromMap(rMesg, "resultMessage").contains("no message available"));
		
		assertTrue("wrong result message",
				getValueFromMap(rMesg, "resultMessage").equals("message retrieved"));
		
		assertTrue("sending or retrieving message failed", 
				getValueFromMap(rMesg, "message").equals("myMessage"));
	}
	
}
