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
		Map<String, String> mqMessage = new HashMap<String, String>();
		
		// q.send(mqHost, port, user, pass, virtualHost, queue, message)
		q.send("localhost", "", "", "", "", "junit", "myMessage");
				
		mqMessage = q.retrieve("localhost", "", "", "", "", "junit");
		String mesg = getValueFromMap(mqMessage, "message");
		
		assertTrue("sending or retrieving message failed", "myMessage".equalsIgnoreCase(mesg));
	}

}
