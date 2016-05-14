package xdi2.core.impl.json.mongodb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ServerAddress;

import xdi2.core.GraphFactory;
import xdi2.core.impl.json.AbstractJSONGraphFactory;
import xdi2.core.impl.json.JSONStore;

/**
 * GraphFactory that creates JSON graphs in MongoDB.
 * 
 * @author markus
 */
public class MongoDBJSONGraphFactory extends AbstractJSONGraphFactory implements GraphFactory {

	private static final Logger log = LoggerFactory.getLogger(MongoDBJSONGraphFactory.class);

	public static final String  DEFAULT_HOST = "localhost";
	public static final Integer DEFAULT_PORT = null;

	private String  host;
	private Integer port;
	private Boolean mockFlag;
	private Boolean hashIdentifierFlag;
	private Boolean sharedDatabaseFlag;
	private List<ServerAddress> replicaSet;
	private MongoClientOptions mongoClientOptions;

	public MongoDBJSONGraphFactory() { 

		super();

		this.host = DEFAULT_HOST;
		this.port = DEFAULT_PORT;
		this.mockFlag = Boolean.FALSE;
		this.hashIdentifierFlag = Boolean.FALSE;
		this.sharedDatabaseFlag = Boolean.TRUE;
	}

	@Override
	protected JSONStore openJSONStore(String identifier) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("openJSONStore for identifier " + identifier);
		}

		if (Boolean.TRUE.equals(this.getHashIdentifierFlag())) {
			identifier = hashIdentifier(identifier);
		}

		MongoClient mongoClient = null;

		if (replicaSet != null) {
			mongoClient = getMongoClientFromReplicaSet(replicaSet, mongoClientOptions);
		} else {
			mongoClient = getMongoClient(this.getHost(), this.getPort());
		}

		JSONStore jsonStore = new MongoDBJSONStore(mongoClient, identifier, this.getMockFlag(), this.getSharedDatabaseFlag());
		jsonStore.init();

		return jsonStore;
	}

	private static HashMap<String, MongoClient> mongoClients = new HashMap<String, MongoClient>();

	private static String getMongoClientKey(String host, Integer port) {
		String key = host;
		if (port != null) {
			key = key + ":" + port;
		}
		return key;
	}

	public static synchronized MongoClient getMongoClient(String host, Integer port) {
		if (log.isTraceEnabled()) {
			log.trace("getMongoClient() " + host + " " + port);
		}
		String key = getMongoClientKey(host, port);
		MongoClient rtn = mongoClients.get(key);
		if (rtn != null) {
			return rtn;
		}
		try
		{
			MongoClient client = null;
			if (port != null) {
				client = new MongoClient(host, port.intValue());
			} else {
				client = new MongoClient(host);
			}
			rtn = client;
			mongoClients.put(key, rtn);
		} catch (java.net.UnknownHostException e) {
			log.error("getMongoClient() " + host + " " + port + " failed - " + e, e);
			rtn = null;
		}
		return rtn;
	}

	public static synchronized MongoClient getMongoClientFromReplicaSet(List<ServerAddress> replicaSet, MongoClientOptions clientOptions) {

		String key = getMongoClientKey(replicaSet.hashCode() + "", null);
		MongoClient rtn = mongoClients.get(key);
		if (rtn != null) {
			return rtn;
		}

		MongoClient client = null;
		client = new MongoClient(replicaSet, clientOptions);
		rtn = client;
		mongoClients.put(key, rtn);

		return rtn;
	}

	public static String hashIdentifier(String identifier) {

		try {

			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			return URLSafe(new String(Base64.encodeBase64(digest.digest(identifier.getBytes(StandardCharsets.UTF_8))), StandardCharsets.UTF_8));
		} catch (Exception ex) {

			log.error("hashIdentifier " + identifier + "failed", ex);
			return identifier;
		}
	}

	private static String URLSafe(String string) {

		return string.replace('+', '-').replace('/', '_');
	}

	/*
	 * Getters and setters
	 */

	public String getHost() {

		return this.host;
	}

	public void setHost(String host) {

		this.host = host;
	}

	public Integer getPort() {

		return this.port;
	}

	public void setPort(Integer port) {

		this.port = port;
	}

	public Boolean getMockFlag() {
		return this.mockFlag;
	}

	public void setMockFlag(Boolean mockFlag) {
		this.mockFlag = mockFlag;
	}

	public Boolean getHashIdentifierFlag() {
		return this.hashIdentifierFlag;
	}

	public void setHashIdentifierFlag(Boolean hashIdentifierFlag) {
		this.hashIdentifierFlag = hashIdentifierFlag;
	}

	public Boolean getSharedDatabaseFlag() {
		return this.sharedDatabaseFlag;
	}

	public void setSharedDatabaseFlag(Boolean sharedDatabaseFlag) {
		this.sharedDatabaseFlag = sharedDatabaseFlag;
	}

	/**
	 * @return the replicaSet
	 */
	public List<ServerAddress> getReplicaSet() {
		return replicaSet;
	}

	/**
	 * @param replicaSet the replicaSet to set
	 */
	public void setReplicaSet(List<ServerAddress> replicaSet) {
		this.replicaSet = replicaSet;
	}

	/**
	 * @return the mongoClientOptions
	 */
	public MongoClientOptions getMongoClientOptions() {
		return mongoClientOptions;
	}

	/**
	 * @param mongoClientOptions the mongoClientOptions to set
	 */
	public void setMongoClientOptions(MongoClientOptions mongoClientOptions) {
		this.mongoClientOptions = mongoClientOptions;
	}


}
