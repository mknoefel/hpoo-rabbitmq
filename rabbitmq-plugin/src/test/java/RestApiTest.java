import static org.junit.Assert.*;

import org.junit.Test;


public class RestApiTest {

	
	@Test
	public void RestApiInitTest() {
		RestApi host = new RestApi();
		String user = "admin";
		String pass = "admin";
		String result = "";
		
		try {
			result = host.httpGet("https://oo10.fritz.box:8443/oo/rest/authns", user, pass);
		} catch (Exception e) {
			fail("could not set up link");
		}
		
		assertTrue("could not get authn", result.contains("enable"));
		
		String flowId ="55acde5e-a562-43d9-a3c9-d325d8b98621";
		try {
			result = host.httpGet("https://oo10.fritz.box:8443/oo/rest/flows/"+flowId+"/inputs", user, pass);
		} catch (Exception e) {
			System.out.println("exception: "+e.getMessage());
			fail("could not set up link");
		}
		
		String myText = "{\"uuid\":\"55acde5e-a562-43d9-a3c9-d325d8b98621\"," +
				  "\"runName\": \"MyRabbitMQRun\", \"logLevel\": \"DEBUG\"," +
				  "\"inputs\":{\"password\":\"Test1234\"}}";
		
		try {
			result = host.httpPost("https://oo10.fritz.box:8443/oo/rest/executions/", user, pass, myText);
		} catch (Exception e) {
			fail("could not set up link");
		}
		
		assertTrue("POST seems not to work", result.contains("feedUrl"));
	}

}