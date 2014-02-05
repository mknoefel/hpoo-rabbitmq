import static org.junit.Assert.*;

import java.util.Map;
import java.util.HashMap;

import org.junit.Test;


public class rmqTest {

	@Test
	public void test() {
		rmq q = new rmq();
		Map<String, String> mqMessage = new HashMap<String, String>();
		
		// q.send(mqHost, port, user, pass, virtualHost, queue, message)
		q.send("localhost", "", "", "", "", "junit", "message");
				
		mqMessage = q.retrieve("localhost", "", "", "", "", "junit");
		String mesg = new String();
		
		for (Map.Entry<String, String> entry: mqMessage.entrySet()) {
			System.out.printf("key: %s, value <%s>\n", 
					entry.getKey().toString(), 
					entry.getValue().toString());
			if ("message".equals(entry.getKey())) {
				mesg = entry.getValue().toString();
			}
		}
		assertTrue("sending or retrieving message failed", "message".equalsIgnoreCase(mesg));
	}

}
