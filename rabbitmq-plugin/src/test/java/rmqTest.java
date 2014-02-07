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
		
		// q.send(message, mqHost, port, user, pass, virtualHost, exchange, queue)
		q.send("myMessage", "localhost", "", "oo", "Test1234", "ooHost", "", "junit",
				"", "", "", "", "", "", "", "", "", "", "", "dd.MM.yyyy", "", "", "");
				
		mqMessage = q.retrieve("localhost", "", "oo", "Test1234", "ooHost", "junit", "true");
		String mesg = getValueFromMap(mqMessage, "message");
		
		assertTrue("sending or retrieving message failed", "myMessage".equalsIgnoreCase(mesg));
	}
	
}
