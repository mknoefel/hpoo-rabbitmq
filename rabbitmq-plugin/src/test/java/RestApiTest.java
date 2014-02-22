import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;


public class RestApiTest {

	
	@Test
	public void RestApiInitTest() {
		RestApi rest = new RestApi();
		String user = "admin";
		String pass = "admin";
		String result = "";
		
		try {
			result = rest.httpGet("https://oo10.fritz.box:8443/oo/rest/authns", user, pass);
		} catch (Exception e) {
			fail("could not set up link");
		}
		
		assertTrue("could not get authn", result.contains("enable"));
		
		String flowId ="55acde5e-a562-43d9-a3c9-d325d8b98621";
		try {
			result = rest.httpGet("https://oo10.fritz.box:8443/oo/rest/flows/"+flowId+"/inputs", user, pass);
		} catch (Exception e) {
			System.out.println("exception: "+e.getMessage());
			fail("could not set up link");
		}
		
		String myText = "{\"uuid\":\"55acde5e-a562-43d9-a3c9-d325d8b98621\"," +
				  "\"runName\": \"MyRabbitMQRun\", \"logLevel\": \"DEBUG\"," +
				  "\"inputs\":{\"noop\":\"junitTest\"}}";
		
		try {
			result = rest.httpPost("https://oo10.fritz.box:8443/oo/rest/executions/", user, pass, myText);
		} catch (Exception e) {
			fail("could not set up link");
		}
		
		assertTrue("POST seems not to work", result.contains("feedUrl"));
	}
	
	@Test
	public void getInputMapTest()
	{
		RestApi rest = new RestApi();
		Map<String,Boolean> map = new HashMap<String,Boolean>();
		String host = "oo10.fritz.box";
		String port = "8443";
		String username = "admin";
		String password = "admin";
		String flowId = "388f4cfa-62ca-46a3-bf03-9ab0561878de";
		// String flowId = "55acde5e-a562-43d9-a3c9-d325d8b98621";
		
		map = rest.getFlowInputMap(host, port, username, password, flowId, true);
		
		assertTrue("'Find Inactive Domain Administrators' inputs could not be read",
				map.size() == 3);
	}
	
	@Test
	public void getFlowNameTest() 
	{
		RestApi rest = new RestApi();
		String host = "oo10.fritz.box";
		String port = "8443";
		String username = "admin";
		String password = "admin";
		String flowId = "388f4cfa-62ca-46a3-bf03-9ab0561878de";
		String flowName = "";
		
		flowName = rest.getFlowName(host, port, username, password, flowId);
		assertTrue("wrong name", flowName.equals("Find Inactive Domain Administrators"));
	}

}