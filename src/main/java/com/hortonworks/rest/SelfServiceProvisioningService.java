package com.hortonworks.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasException;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.requests.MetadataResponse.PartitionMetadata;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import kafka.utils.ZkUtils;
import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;

@Path("/provisioning")
public class SelfServiceProvisioningService {
		public static final String DEFAULT_ATLAS_REST_ADDRESS = "http://sandbox.hortonworks.com";
		public static final String DEFAULT_ADMIN_USER = "admin";
		public static final String DEFAULT_ADMIN_PASS = "admin";
		private static AtlasClient atlasClient;
		private String[] basicAuth = {DEFAULT_ADMIN_USER, DEFAULT_ADMIN_PASS};
		private Properties props = System.getProperties();
		private static java.sql.Connection phoenixConnection;
		private static String shredderUrl = "http://loanmaker01-238-3-2:8095";
		private static String nifiHost = "hdf01";
		private static String nifiPort = "9090";
		//Leave NifiUrl Blank, it is created from nifiHost and nifiPort
		private static String nifiUrl = "";
		private static String registryUrl = "http://hdf01:9095";
		private static String atlasUrl = "http://loanmaker01-238-3-2:21000";
		private static String zkKafkaUri = "hdf01:2181";
		private static String zkHbaseUri = "hdf01:2182:/hbase";
		private String hostName;
		
		public SelfServiceProvisioningService(@Context ServletContext servletContext){
		//Need to pass the various intergration points from webservice Main method, config file, or hardcode defaults 
			//zkKafkaUri = servletContext.getInitParameter("zkKafkaUri");
			//zkHbaseUri = servletContext.getInitParameter("zkHbaseUri");
			//shredderUrl = servletContext.getInitParameter("shredderUrl");
			//nifiHost = servletContext.getInitParameter("nifiHost");
			//nifiPort = servletContext.getInitParameter("nifiPort");
			//registryUrl = servletContext.getInitParameter("registryUrl");
			//atlasUrl = servletContext.getInitParameter("atlasUrl");
	       	
			props.setProperty("atlas.conf", "/Users/vvaks/"); //All that is required is that a file called atlas-application.properties is present at this location. It can be empty with a single character.
			try {
				hostName = InetAddress.getLocalHost().getHostName();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
			String[] atlasURL = {atlasUrl};
			atlasClient = new AtlasClient(atlasURL, basicAuth);
			System.out.println("***************** atlas.conf has been set to: " + props.getProperty("atlas.conf"));
		}
		
		@GET
		@Produces(MediaType.APPLICATION_JSON)
		@Path("getSchema/{guid}")
		public Response getSchema(@PathParam("guid") String guid){
			//No-Op GET method... ignore
			return Response.status(200).entity("").build();
		}
		
		@POST
		@Produces(MediaType.TEXT_PLAIN)
		@Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN, MediaType.WILDCARD})
		@Path("provisionStream")
		public Response storeSchema(@QueryParam("tableName") String tableName, 
									@QueryParam("topicName") String topicName,
									@QueryParam("schemaGroup") String schemaGroup,
									String jsonData) {
			String result = "Data post: " + jsonData;
			System.out.println(result);
			
			nifiUrl = "http://" + nifiHost + ":" + nifiPort;
			try {
				Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
				phoenixConnection = DriverManager.getConnection("jdbc:phoenix:"+ zkHbaseUri);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
			String schemaName = topicName + ":v";
			String schemaText = jsonData;
			String schemaDescription = null;
			String topicUri = null;
			
			//create Phoenix Table
		    createPhoenixTable(tableName);
		    
		    //register Schema Group and Schema Version in Schema Registry
		    String schemaGroupId = registerNewSchemaGroup(schemaName, schemaGroup);
		    String schemaVersionId = registerNewSchemaVersion(schemaName, schemaText, schemaDescription);
		    System.out.println("Schema Group Id: " + schemaGroupId + " Schema Version Id:" + schemaVersionId);
		    
		    //Store Schema in Atlas... 
		    //This requires the AvroSchemaShredder Webservice to be running on the same host as the Atlas Server
		    	//https://github.com/vakshorton/AvroSchemaShredder.git
		    	//https://community.hortonworks.com/articles/51432/apache-atlas-as-an-avro-schema-registry-test-drive.html
		    Referenceable schemaReferenceable = storeSchemaInAtlas(schemaText);
		    
		    //Create Kafka Topic and register Kafka Topic meta data in Atlas
		    Referenceable kafkaTopicReferenceable = createKafkaTopic(topicName, schemaReferenceable);
		    
		    
		    topicUri = kafkaTopicReferenceable.get("uri").toString();
		    System.out.println("Topic Uri: " + topicUri);
		    createFlow(tableName, topicUri, topicName, schemaGroup, schemaName);
		
			return Response.status(200).entity("{\"KafkaTopicUri\":\""+ topicUri +"\",\"KafkaTopicRestUrl\":\"http://"+ nifiHost +":8083/" + topicName + "\",\"TableConnectionString\": \"jdbc:phoenix:" + zkHbaseUri  + "\"}").build();		 
		}
		
		private Referenceable createKafkaTopic(String topicName, Referenceable schemaReferenceable){
		    int sessionTimeoutMs = 10 * 1000;
		    int connectionTimeoutMs = 8 * 1000;
		    // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
		    // createTopic() will only seem to work (it will return without error).  The topic will exist in
		    // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
		    // topic.
		    ZkClient zkClient = new ZkClient(zkKafkaUri,sessionTimeoutMs,connectionTimeoutMs, ZKStringSerializer$.MODULE$);

		    // Security for Kafka was added in Kafka 0.9.0.0
		    boolean isSecureKafkaCluster = false;
		    ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(zkKafkaUri), isSecureKafkaCluster);

		    int partitions = 1;
		    int replication = 1;
		    Properties topicConfig = new Properties(); // add per-topic configurations settings here
		    Referenceable kafkaTopic = new Referenceable("kafka_topic");
		    
		    try{
		    	//System.out.println("Checking if Kafka Topic exists: " +AdminUtils.topicExists(zkUtils, topicName));
		    	AdminUtils.createTopic(zkUtils, topicName, partitions, replication, topicConfig, null);
		    	topicName = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils).topic();
		    }catch(TopicExistsException e){
		    	System.out.println("Checking if Kafka Topic exists: true");
			}
		    List<Referenceable> schemas = new ArrayList<Referenceable>();
		    schemas.add(schemaReferenceable);
		    List<PartitionMetadata> topicParitionData = AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils).partitionMetadata();
		    
		    String host = topicParitionData.get(0).leader().host();
		    String port = String.valueOf(topicParitionData.get(0).leader().port());
		    zkClient.close();
		    
		    System.out.println("Kafka Topic Name: " + topicName);
	    	System.out.println("Kafka Topic Host: " + host);
	    	System.out.println("Kafka Topic Port: " + port);
	    
	    	kafkaTopic.set("name", topicName);
	    	kafkaTopic.set("description", topicName);
	    	kafkaTopic.set("uri", host + ":" + port);
	    	kafkaTopic.set("qualifiedName", topicName + "@" + hostName);
	    	kafkaTopic.set("avro_schema", schemas);
	    	kafkaTopic.set("topic", topicName);
	    	kafkaTopic.set("owner", "HDF");
	    
	    	try {
				atlasClient.createEntity(InstanceSerialization.toJson(kafkaTopic, true));
			} catch (AtlasServiceException e) {
				e.printStackTrace();
			}
		    
			return kafkaTopic;
		}
		
		private String registerNewSchemaGroup(String schemaName, String schemaGroup){
			String output = null;
	    	String string = null;
			String payload = "{" +
								"\"type\": \"avro\", " +
								"\"schemaGroup\": \"" + schemaGroup + "\"," +
								"\"name\": \"" + schemaName +"\"," + 
								"\"description\": null," +
								"\"compatibility\": \"BACKWARD\" " +
							 "}";
			String schemaEntityId = null;
			try{
		    	URL url = new URL(registryUrl + "/api/v1/schemaregistry/schemas");
		    	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		        System.out.println("Schema Group Payload: " + payload);    
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}
		    	schemaEntityId = new JSONObject(string).get("entity").toString();
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			return schemaEntityId;
		}
		
		private String registerNewSchemaVersion(String schemaName, String schemaText, String schemaDescription){
			String output = null;
	    	String string = null;
	    	String schemaVersionEntityId = null;
	    	System.out.println(schemaText.toString());
			
		    String payload = "{\"description\": \"" + schemaDescription + "\"," +
		    				  "\"schemaText\": " + JSONObject.quote(schemaText) + "}";
		    System.out.println(payload.toString());
			try{
		    	URL url = new URL(registryUrl + "/api/v1/schemaregistry/schemas/" + schemaName + "/versions");
		    	System.out.println("Url: " + url);
		    	HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    	
		    	schemaVersionEntityId = new JSONObject(string).get("entity").toString();
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			return schemaVersionEntityId;
		}
		
		private void createPhoenixTable(String tableName) {
			//The Phoenix table schema is the only part of this demo that is currently hard coded
			//Need to implement a recursive function to convert Avro Schema into SQL DLL statement
			String sql = " CREATE TABLE IF NOT EXISTS \"" + tableName +"\" ("
												+ "\"firstname\" VARCHAR NOT NULL, "
												+ "\"lastname\" VARCHAR NOT NULL, "
												+ "CONSTRAINT \"pk\" PRIMARY KEY (\"firstname\",\"lastname\"))";
			System.out.println("Phoenix Table DDL: " + sql);
			try {
				Statement create = phoenixConnection.createStatement();
				int createRS = create.executeUpdate(sql);
				create.close();			
				if (createRS > 0) {
					System.out.println("Successfully created table");
				}
				phoenixConnection.commit();
				phoenixConnection.close();
			} catch (SQLException e) {
				try {
					phoenixConnection.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
		}
		
		private void createFlow(String tableName, String kafkaUri, String topicName, String schemaGroup, String schemaName){
			List<String> processorIds = new ArrayList<String>();
			String payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":0.0,\"y\":600.0}," +
					 	 "\"config\":" +
							"{\"properties\":" +
								"{\"Base Path\":\"" + topicName + "\"," +
									"\"Listening Port\": \"8083\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\"]" +
							"}," +
						"\"name\": \"Listen on HTTP\"," +
						"\"type\": \"org.apache.nifi.processors.standard.ListenHTTP\"," +
						"\"state\":\"STOPPED\"" +
					 "}" +
				 "}";
			System.out.println("Creating ListenHTTP Processor: " + payload.toString());
			String listenerId = null;
			try {
				listenerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(listenerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":600.0,\"y\":600.0}," +
					 	 "\"config\":" +
							"{\"properties\":" +
								"{\"schema-registry-service\":\"28e3587e-015a-1000-ffff-ffff93a89c40\"," +
									"\"schema-name\": \"" + schemaName + "\"," +
									"\"schema-version\": null," +
									"\"schema-type\":\"avro\"," +
									"\"schema-group\":\"NIFI\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\"]" +	
							"}," +		
						"\"name\": \"Serializer\"," +
						"\"type\": \"com.hortonworks.nifi.registry.TransformJsonToAvroViaSchemaRegistry\"," +
						"\"state\":\"STOPPED\"" +
					 "}" +
				 "}";
			System.out.println("Creating Serializer Processor: " + payload.toString());
			String serializerId = null;
			try {
				serializerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(serializerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + listenerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + serializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting Deserializer and KafkaProducer Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":1200.0,\"y\":600.0}," +
					 	"\"config\":" +
							"{\"properties\":" +
								"{\"bootstrap.servers\":\"" + kafkaUri + "\"," +
									"\"topic\": \"" + topicName + "\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"success\"]" +
							"}," +
							"\"name\": \"Publish To Custom Topic\"," +
							"\"type\": \"org.apache.nifi.processors.kafka.pubsub.PublishKafka_0_10\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating Kafka Producer Processor: " + payload.toString());
			String kafkaProducerId = null;
			try {
				kafkaProducerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(kafkaProducerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + serializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + kafkaProducerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting Deserializer and KafkaProducer Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":0.0,\"y\":300.0}," +
					 	"\"config\":" +
							"{\"properties\":" +
								"{\"bootstrap.servers\":\"" + kafkaUri + "\"," +
									"\"topic\": \"" + topicName + "\"," +
									"\"group.id\":\"NIFI\"" +
								"}" +
							"}," +
							"\"name\": \"Consume Custom Topic\"," +
							"\"type\": \"org.apache.nifi.processors.kafka.pubsub.ConsumeKafka_0_10\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating Kafka Consumer Processor: " + payload.toString());
			String kafkaConsumerId = null;
			try {
				kafkaConsumerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(kafkaConsumerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":600.0,\"y\":300.0}," +
					 	 "\"config\":" +
							"{\"properties\":" +
								"{\"schema-registry-service\":\"28e3587e-015a-1000-ffff-ffff93a89c40\"," +
									"\"schema-name\": \"" + schemaName + "\"," +
									"\"schema-version\": null," +
									"\"schema-type\":\"avro\"," +
									"\"schema-group\":\"NIFI\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\"]" +
							"}," +	
						"\"name\": \"Deserializer\"," +
						"\"type\": \"com.hortonworks.nifi.registry.TransformAvroToJsonViaSchemaRegistry\"," +
						"\"state\":\"STOPPED\"" +
					 "}" +
				 "}";
			System.out.println("Creating Deserializer Processor: " + payload.toString());
			String deserializerId = null;
			try {
				deserializerId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(deserializerId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + kafkaConsumerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + deserializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting KafkaConsumer and Deserializer Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":1200.0,\"y\":300.0}," +
					 		"\"config\":" +
							"{\"properties\":" +
								"{\"JDBC Connection Pool\":\"28e596aa-015a-1000-ffff-ffffa3d8a71a\"," +
									"\"Statement Type\": \"INSERT\"," +
									"\"Table Name\": \""+ tableName +"\"," +
									"\"jts-quoted-identifiers\": true" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"original\"]" +
							"}," +	
							"\"name\": \"Create Insert Statement\"," +
							"\"type\": \"org.apache.nifi.processors.standard.ConvertJSONToSQL\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating JsonToSQL Processor: " + payload.toString());
			String jsonToSQLId = null;
			try {
				jsonToSQLId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(jsonToSQLId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + deserializerId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + jsonToSQLId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting Deserializer and JsonToSQL Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":1800.0,\"y\":300.0}," +
					 		"\"config\":" +
							"{\"properties\":" +
								"{\"Regular Expression\":\"INSERT INTO " + tableName +"\"," +
								 "\"Replacement Value\": \"UPSERT INTO \\\"" + tableName + "\\\"\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"original\"]" +
							"}," +	
							"\"name\": \"Modify Insert Syntax\"," +
							"\"type\": \"org.apache.nifi.processors.standard.ReplaceText\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating ReplaceText Processor: " + payload.toString());
			String replaceTextId = null;
			try {
				replaceTextId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(replaceTextId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + jsonToSQLId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + replaceTextId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"sql\"]," +
						"\"availableRelationships\": [\"sql\"] " +
						"}" +
				 "}";
			System.out.println("Connecting JsonToSQL to ReplaceText Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"position\":{\"x\":2400.0,\"y\":300.0}," +
					 		"\"config\":" +
							"{\"properties\":" +
								"{\"JDBC Connection Pool\":\"28e596aa-015a-1000-ffff-ffffa3d8a71a\"" +
								"}," +
								"\"autoTerminatedRelationships\": [\"failure\",\"retry\",\"success\"]" +
							"}," +	
							"\"name\": \"Persist to Phoenix\"," +
							"\"type\": \"org.apache.nifi.processors.standard.PutSQL\"," +
							"\"state\":\"STOPPED\"" +
						"}" +
				 "}";
			System.out.println("Creating PutToSQL Processor: " + payload.toString());
			String putSQLId = null;
			try {
				putSQLId = new JSONObject(postRequestNifi(payload, "processors")).get("id").toString();
				processorIds.add(putSQLId);
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			payload = "{\"revision\":" +
					"{\"clientId\":\"x\"," +
					 "\"version\":0" +
					 "}," +
					 "\"component\":" +
					 	"{\"source\":" +
							"{\"id\": \"" + replaceTextId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"destination\":" +
							"{\"id\": \"" + putSQLId + "\"," +
								"\"type\": \"PROCESSOR\"" +
							"}," +
						"\"selectedRelationships\": [\"success\"]," +
						"\"availableRelationships\": [\"success\"] " +
						"}" +
				 "}";
			System.out.println("Connecting JsonToSQL to PutSQL Processors: " + payload.toString());
			postRequestNifi(payload, "connections");
			
			Iterator<String> idIterator = processorIds.iterator();
			while(idIterator.hasNext()){
				String currentProcessorId = idIterator.next();
				payload = "{\"revision\":" +
							"{" +
								"\"clientId\":\"x\"," +
								"\"version\":1" +
							"}," +
							"\"id\": \""+ currentProcessorId +"\"," +
							"\"component\":" +
						 	"{" +
						 		"\"id\": \""+ currentProcessorId +"\"," +
								"\"state\":\"RUNNING\"" +
							"}" +
						"}";
				putRequestNifi(currentProcessorId, payload, "processors");
			}
		}
		
		private String postRequestNifi(String payload, String requestType){
			String output = null;
			String string = null;
			try {
				URL url = new URL(nifiUrl + "/nifi-api/process-groups/root/" + requestType);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				System.out.println("Url: " + url);
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			
			return string;
		}
		
		private String putRequestNifi(String id, String payload, String requestType){
			String output = null;
			String string = null;
			try {
				URL url = new URL(nifiUrl + "/nifi-api/" + requestType + "/" + id);
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				System.out.println("Url: " + url);
		    	conn.setDoOutput(true);
		    	conn.setRequestMethod("PUT");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println(string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
			System.out.println("Response: " + string);
			
			return string;
		}
		
		private JSONArray getAllAvroSchemasFromRegistry(){
			URL url;
			JSONArray schemas = null;
			try {
				url = new URL(registryUrl + "/api/v1/schemaregistry/schemas");
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setDoOutput(true);
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Accept", "application/json");
	    
	    		if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
				}	

	    		BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));

				String output = null;
				String string = null;
				System.out.println("Output from Server .... \n");
				while ((output = br.readLine()) != null) {
					string = output;
				}
				JSONObject schemasResponse = new JSONObject(string);
				schemas = schemasResponse.getJSONArray("entities");
				System.out.println(schemas);
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (ProtocolException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}
			
			return schemas;
		}
		
		private String getAvroSchemaFromRegistry(String schemaName){
			String schemaText = null;
			URL url;
			try {
				url = new URL(registryUrl + "/api/v1/schemaregistry/schemas/"+schemaName+"/versions/latest");
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    		conn.setDoOutput(true);
		    	conn.setRequestMethod("GET");
		    	conn.setRequestProperty("Accept", "application/json");
		            
		        if (conn.getResponseCode() != 200) {
		        	throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}

		        BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		        String output = null;
		        String string = null;
		        System.out.println("Output from Server .... \n");
		        while ((output = br.readLine()) != null) {
		        	string = output;
		        }
		        //System.out.println(string);
		        JSONObject schemasResponse = new JSONObject(string);
			    JSONObject schema = schemasResponse.getJSONObject("entity");
			    schemaText = schema.getString("schemaText").replaceAll("\\n", "").replaceAll("\\s+","");
			    System.out.println(schemaText);
			} catch (MalformedURLException e) {
					e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JSONException e) {
				e.printStackTrace();
			}     
		        	
		    return schemaText;    	
		}
		
		private Referenceable storeSchemaInAtlas(String payload){
			//Make sure to start the AvroSchemaShredder Webservice on the same host that is running the Atlas Server
	    	//https://github.com/vakshorton/AvroSchemaShredder.git
	    	//https://community.hortonworks.com/articles/51432/apache-atlas-as-an-avro-schema-registry-test-drive.html
			System.out.println("Storing Schema in Atlas...");
	        System.out.println(payload.toString());
	        String output = null;
	    	String string = null;
	    	Referenceable schemaRefernceable = null;
	        try{
	        	URL url = new URL(shredderUrl + "/schemaShredder/storeSchema");
	    		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    		conn.setDoOutput(true);
		    	conn.setRequestMethod("POST");
		    	conn.setRequestProperty("Content-Type", "application/json");
		            
		    	OutputStream os = conn.getOutputStream();
		    	os.write(payload.getBytes());
		    	os.flush();
		            
		    	BufferedReader br = null;    
		    	if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 205){
		    		br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
		    		while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println("Code: " + conn.getResponseCode() + " : " + string);
		    	}else{
		    		br = new BufferedReader(new InputStreamReader((conn.getErrorStream())));
		            while ((output = br.readLine()) != null) {
		            	string = output;
		            }
		            System.out.println("Code: " + conn.getResponseCode() + " : " + string);
		            throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
		    	}	
		    	
		    } catch (Exception e) {
		            e.printStackTrace();
		    }
	        
	        try {
	        	System.out.println(atlasClient.getAdminStatus());
	        	schemaRefernceable = atlasClient.getEntity(string);
			} catch (AtlasServiceException e1) {
				e1.printStackTrace();
			} catch(Exception e){
				e.printStackTrace();
			}
	        
			System.out.println("Response: " + string);
	        
	        return schemaRefernceable; 
	    }
}
