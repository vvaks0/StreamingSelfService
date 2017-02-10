package com.hortonworks;

import java.net.InetAddress;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.UriBuilder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingSelfService {
	static final Logger LOG = LoggerFactory.getLogger(StreamingSelfService.class);
	public static final String DEFAULT_ADMIN_USER = "admin"; //
	public static final String DEFAULT_ADMIN_PASS = "admin"; //
	private static String shredderUrl = "";
	private static String nifiHost = "";
	private static String nifiPort = "";
	private static String registryUrl = "";
	private static String atlasUrl = "";
	private static String zkKafkaUri = "";
	private static String zkHbaseUri = "";
	
	public static void main(String[] args) throws Exception {
		if(args.length > 0){
			zkKafkaUri = args[0];
			zkHbaseUri = args[1];
			shredderUrl = args[2];
			registryUrl = args[3];
			atlasUrl = args[4];
			nifiHost = args[5];
			nifiPort = args[6];
		}
		
		System.out.println("Starting Webservice...");
		final HttpServer server = startServer();
		server.start();
	}
	
	public static HttpServer startServer() {
       	//Map<String,String> deviceDetailsMap = new HashMap<String, String>();
       	String hostName = "sandbox.hortonworks.com";
		Map<String,String> configMap = new HashMap<String, String>();
       	ResourceConfig config = new ResourceConfig();
       	try {
			hostName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
       	URI baseUri = UriBuilder.fromUri("http://" + hostName + ":8096").build();
       	configMap.put("zkKafkaUri", zkKafkaUri);
       	configMap.put("zkHbaseUri", zkHbaseUri);
       	configMap.put("shredderUrl", shredderUrl);
       	configMap.put("nifiHost", nifiHost);
       	configMap.put("nifiPort", nifiPort);
       	configMap.put("registryUrl", registryUrl);
       	configMap.put("atlasUrl", atlasUrl);
       	
       	config.packages("com.hortonworks.rest");
       	config.setProperties(configMap);
        HttpServer server = GrizzlyHttpServerFactory.createHttpServer(baseUri, config);
   		return server;
    }
}