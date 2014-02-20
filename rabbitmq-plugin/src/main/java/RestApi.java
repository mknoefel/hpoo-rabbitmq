import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.URL;

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
			System.out.println("httpPost: "+conn.getResponseMessage());
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
	
}