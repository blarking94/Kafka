package kafka.KafkaRest;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;


public class App {
 
	private final String USER_AGENT = "Mozilla/5.0";

	public static void main(String[] args) throws Exception {

		App http = new App();

		System.out.println("Testing 1 - Send Http GET request");
		http.getTopics();

		System.out.println("\nTesting 2 - Send Http POST request");
		http.sendTest();

	}

	// HTTP GET request
	private void getTopics() throws Exception {
		// Put your IP here 
		String url = "http://xxx.xxx.xxx:8082/topics";

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		// add request header
		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		System.out.println("\nSending 'GET' request to URL : " + url);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();

		// print result
		System.out.println(response.toString());

	}

	// HTTP POST request
	private void sendTest() throws Exception {
		
		// Put your IP here
		String url = "http://xxx.xxx.xxx.xxx:8082/topics/bentest";

		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		
		// The value is "Kafka" encoded in a base 64 format
		String urlParameters = "{\"records\":[{\"value\":\"S2Fma2E=\"}]}";
		byte[] postData = urlParameters.getBytes(StandardCharsets.UTF_8);

		con.setRequestMethod("POST");
		con.setRequestProperty("Content-Type", "application/vnd.kafka.v1+json");
		con.setRequestProperty("Content-Length", "" + postData.length);
		con.setRequestProperty("Content-Language", "en-US");
		con.setUseCaches(false);
		con.setDoInput(true);
		con.setDoOutput(true);
		
		
		DataOutputStream wr = new DataOutputStream(con.getOutputStream());
		// Write "Kafka" to topic ben test
		try {
			wr.write(postData);
		} catch (Exception e) {
			e.printStackTrace();
		}

		int responseCode = con.getResponseCode();
		
		System.out.println("\nSending 'POST' request to URL : " + url);
		// System.out.println("Post parameters : " + urlParameters);
		System.out.println("Response Code : " + responseCode);

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();

		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		
		in.close();

		// print result
		System.out.println(response.toString());

	}

}