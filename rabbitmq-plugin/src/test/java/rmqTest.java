import static org.junit.Assert.*;

import java.util.Map;
import java.util.HashMap;
import java.util.UUID;

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
	
	private void sleep(int seconds) {
		try {
			Thread.sleep(seconds * 1000);
		} catch (Exception e) { /* do nothing */ }
	}
	
	
	@Test
	public void sendRetrieveAndAckTest() {
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
		
		rMesg = q.retrieve(channelId, null, null, null, null, null, null, queueName, "false", null);
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
		
		q.closeChannel(channelId);
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
	
	@Test
	public void tempQueueTest()
	{
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		
		mesg = q.createTempQueue(channelId);
		assertTrue("channel could not be closed", mesg.get("resultMessage").equals("temporary queue declared"));
		assertTrue("queueName not created correctly", mesg.get("queueName").startsWith("amq.gen"));
		
		mesg = q.closeChannel(channelId);
		assertTrue("channel could not be closed", mesg.get("resultMessage").equals("channel closed"));
	}
	
	@Test
	public void createQueueTest()
	{
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		String queueName = "myTestQ";
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		
		mesg = q.createQueue(channelId, queueName, "true", "false", "false", "{\"Test\":true}");
		assertTrue("queue not created", mesg.get("resultMessage").equals("queue declared"));
		q.closeChannel(channelId);
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		
		mesg = q.createPassiveQueue(channelId, queueName);
		assertTrue("queue not created", mesg.get("resultMessage").equals("queue passively declared"));
		
		String ifUnused = "false";
		String ifEmpty = "false";
		mesg = q.deleteQueue(channelId, queueName, ifUnused, ifEmpty);
		assertTrue("queue not deleted", mesg.get("resultMessage").equals("queue deleted"));
		
		q.closeChannel(channelId);
	}
	
	@Test
	public void createExchangeTest() {
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		String exchange = "myTestEx";
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		
		mesg = q.createExchange(channelId, exchange, null, null, null, null, null);
		assertTrue("exchange not declared", mesg.get("resultMessage").equals("exchange declared"));
		
		String ifUnused = "false";
		mesg = q.deleteExchange(channelId, exchange, ifUnused);
		assertTrue("exchange not deleted", mesg.get("resultMessage").equals("exchange deleted"));
		
		q.closeChannel(channelId);
	}
	
	@Test
	public void bindQueueToExchangeTest()
	{
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		String exchange = "myBindEx";
		String queueName = "myBindQueue";
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		
		mesg = q.createQueue(channelId, queueName, "true", "false", "false", "{\"Test\":true}");
		assertTrue("queue not created", mesg.get("resultMessage").equals("queue declared"));
		
		mesg = q.createExchange(channelId, exchange, null, null, null, null, null);
		assertTrue("exchange not declared", mesg.get("resultMessage").equals("exchange declared"));
		
		mesg = q.bindQueue(channelId, queueName, exchange, queueName, null);
		assertTrue("queue not bound to exchange", mesg.get("resultMessage").equals("queue bound to exchange"));
		
		mesg = q.unbindQueue(channelId, queueName, exchange, queueName, null);
		assertTrue("queue not unbound from exchange", mesg.get("resultMessage").equals("queue unbound from exchange"));
		
		String ifUnused = "false";
		mesg = q.deleteExchange(channelId, exchange, ifUnused);
		assertTrue("exchange not deleted", mesg.get("resultMessage").equals("exchange deleted"));

		String ifEmpty = "false";
		mesg = q.deleteQueue(channelId, queueName, ifUnused, ifEmpty);
		assertTrue("queue not deleted", mesg.get("resultMessage").equals("queue deleted"));
		
		q.closeChannel(channelId);
	}
	
	@Test
	public void bindExchangeToEchangeTest()
	{
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		String sExchange = "mySource";
		String dExchange = "myDestination";
		
		mesg = q.createChannel(mqHost, mqPortString, virtualHost, username, password);
		channelId = mesg.get("channelId");
		
		mesg = q.createExchange(channelId, sExchange, null, null, null, null, null);
		assertTrue("exchange not declared", mesg.get("resultMessage").equals("exchange declared"));
		
		mesg = q.createExchange(channelId, dExchange, null, null, null, null, null);
		assertTrue("exchange not declared", mesg.get("resultMessage").equals("exchange declared"));
		
		mesg = q.bindExchange(channelId, dExchange, sExchange, sExchange, null);
		assertTrue("exchange not bound to exchange", mesg.get("resultMessage").equals("source exchange bound to dest. exchange"));
		
		mesg = q.unbindExchange(channelId, dExchange, sExchange, sExchange, null);
		assertTrue("exchange not unbound from exchange", mesg.get("resultMessage").equals("source exchange unbound from dest. exchange"));
		
		String ifUnused = "false";
		mesg = q.deleteExchange(channelId, sExchange, ifUnused);
		assertTrue("source exchange not deleted", mesg.get("resultMessage").equals("exchange deleted"));

		mesg = q.deleteExchange(channelId, dExchange, ifUnused);
		assertTrue("dest. exchange not deleted", mesg.get("resultMessage").equals("exchange deleted"));
	
		q.closeChannel(channelId);
	}
	
	@Test
	public void createConsumerTest() {
		rmq q = new rmq();
		Map<String, String> mesg = new HashMap<String, String>();
		String channelId = "";
		// String flowUuid = "388f4cfa-62ca-46a3-bf03-9ab0561878de";
		String flowUuid = "55acde5e-a562-43d9-a3c9-d325d8b98621";
		String runName = "";
		String ooHost = "oo10.fritz.box";
		String ooPortString = "8443";
		String ooUsername = "admin";
		String ooPassword = "admin";
		String queueName = "secondQueue";
		
		mesg = q.createConsumer(null, mqHost, mqPortString, username, password, 
					virtualHost, queueName, flowUuid, runName, ooHost, ooPortString, 
					ooUsername, ooPassword, null, null);
		assertTrue("did not create a consumer", mesg.get("resultMessage").equals("consumer created"));
		
		channelId = mesg.get("channelId");
		
		UUID corrId = UUID.randomUUID();
		
		System.out.println("Start: "+corrId.toString());
		
		mesg = q.send("Message one", channelId, "false", mqHost, mqPortString, username, password, virtualHost, exchange, queueName,
				"", "", "", "text/plain", corrId.toString(), "", "", 
				"{\"flowInput\":{\"message\": \"Mesg 1\"}}", 
				"", "", "", "dd.MM.yyyy", "now", "", "");
		
		sleep(1);

		mesg = q.send("Message two", channelId, "false", mqHost, mqPortString, username, password, virtualHost, exchange, queueName,
				"", "", "", "text/plain", corrId.toString(), "", "", 
				"{\"flowInput\":{\"noop\":\"Mesg 2\"}}", 
				"", "", "", "dd.MM.yyyy", "now", "", "");
		
		sleep(2);
		
		System.out.println("Stop");
		
		q.closeChannel(channelId);
	}
}
