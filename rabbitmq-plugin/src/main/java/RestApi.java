import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.tools.json.JSONReader;
import com.rabbitmq.tools.json.JSONWriter;
import com.sun.jersey.core.util.Base64;


public class RestApi {
	/*
	 *  this httpGet is used for the REST API of Operations Orchestration.
	 *  If you did not set up security you might a "CertificateException".
	 */
	public String httpGet(String urlStr, String user, String pass) throws IOException {
		
		URL url = new URL(urlStr);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	
		if (user != null && pass != null) {
			String userpass = user+":"+pass;
			new Base64();
			String basicAuth = "Basic " + new String(Base64.encode(userpass.getBytes()));
			conn.setRequestProperty ("Authorization", basicAuth);
		}
			
		if (conn.getResponseCode() != 200) {
			throw new IOException(conn.getResponseMessage());
		}

		// Buffer the result into a string
		BufferedReader rd = new BufferedReader(
				new InputStreamReader(conn.getInputStream()));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = rd.readLine()) != null) {
			sb.append(line);
		}
		rd.close();
		
		conn.disconnect();
		return sb.toString();
	}
	
	public String httpPost(String urlString, String user, String pass, String postDoc) throws Exception {
		URL url = new URL(urlString);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			  
		if (user != null && pass != null) {
			String userpass = user+":"+pass;
			new Base64();
			String basicAuth = "Basic " + new String(Base64.encode(userpass.getBytes()));
			conn.setRequestProperty ("Authorization", basicAuth);
		}
			  		
		conn.setRequestMethod("POST");
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setUseCaches(false);
		conn.setAllowUserInteraction(false);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
			  

		// Create the form content
		OutputStream out = conn.getOutputStream();
		Writer writer = new OutputStreamWriter(out, "UTF-8");
			  
		writer.write(postDoc);
			  
		writer.close();
		out.close();
			 	
		/* OO POST Execute Flow by UUID returns 201 for success */
		if (conn.getResponseCode() != 201) {
			//System.out.println("httpPost: "+conn.getResponseMessage());
			throw new IOException(conn.getResponseMessage());
		}

		// Buffer the result into a string
		BufferedReader rd = new BufferedReader(
				new InputStreamReader(conn.getInputStream()));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = rd.readLine()) != null) {
			sb.append(line);
		}
		rd.close();

		conn.disconnect();
		return sb.toString();
	}
	
	
	@SuppressWarnings("unchecked")
	public Map<String,Boolean> getFlowInputMap(String host, String port, 
			String user, String pass, String flowId, Boolean onlyMandatory)
	{
		JSONReader rdr = new JSONReader();
		JSONWriter wtr = new JSONWriter();
		String result = "";
		
		Map<String,Boolean> inputMap = new HashMap<String,Boolean>();
		
		try {
			result = httpGet("https://"+host+":"+port+"/oo/rest/flows/"+flowId+"/inputs", user, pass);
		} catch (Exception e) {
			System.out.println("exception: "+e.getMessage());
			fail("could not set up link");
		}
	
		if (rdr.read(result) instanceof ArrayList) {
			ArrayList<String> resultObj = new ArrayList<String>();
			resultObj = (ArrayList<String>) rdr.read(result);
		
			int size = resultObj.size();
		
			for (int index=0; index < size; index++){
				String next = wtr.write(resultObj.get(index));
				Map<String,Object> nextObj = new HashMap<String,Object>();
			
				nextObj = (Map<String, Object>) rdr.read(next);
				if (onlyMandatory && (Boolean) nextObj.get("mandatory")) {
					inputMap.put((String) nextObj.get("name"), (Boolean) nextObj.get("mandatory"));
				}
				
				if (!onlyMandatory)	{
					inputMap.put((String) nextObj.get("name"), (Boolean) nextObj.get("mandatory"));
				}
			}
		}
		
		return inputMap;
	}
	
	@SuppressWarnings("unchecked")
	public String getFlowName(String host, String port, 
			String user, String pass, String flowId)
	{
		JSONReader rdr = new JSONReader();
		String result = "";
		String flowName = "";
		
		try {
			result = httpGet("https://"+host+":"+port+"/oo/rest/flows/"+flowId, user, pass);
		} catch (Exception e) {
			System.out.println("exception: "+e.getMessage());
			fail("could not set up link");
		}
		
		if (rdr.read(result) instanceof HashMap) {
			flowName = ((Map<String, String>) rdr.read(result)).get("name");
		}
		
		return flowName;
	}
}