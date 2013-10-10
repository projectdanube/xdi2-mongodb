package xdi2.core.impl.json.mongodb;


import java.io.IOException;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdi2.core.GraphFactory;
import xdi2.core.impl.json.AbstractJSONGraphFactory;
import xdi2.core.impl.json.JSONStore;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * GraphFactory that creates JSON graphs in MongoDB.
 * 
 * @author markus
 */
public class MongoDBJSONGraphFactory extends AbstractJSONGraphFactory implements GraphFactory {

	private static final Logger log = LoggerFactory.getLogger(MongoDBJSONGraphFactory.class);

	public static final String DEFAULT_HOST = "localhost";
	public static final Integer DEFAULT_PORT = null;

	private String host;
	private Integer port;

	public MongoDBJSONGraphFactory() { 

		super();

		this.host = DEFAULT_HOST;
		this.port = DEFAULT_PORT;
	}

	@Override
	protected JSONStore openJSONStore(String identifier) throws IOException {

		if (identifier == null) identifier = UUID.randomUUID().toString();

		// check identifier

		String dbName = MongoDBJSONStore.toMongoDBName(identifier);

		if (log.isDebugEnabled()) log.debug("DBName for identifier " + identifier + " is " + dbName);

		// create mongo client

		MongoClient mongoClient;

		if (this.getPort() != null) {

			mongoClient = new MongoClient(this.getHost(), this.getPort().intValue());
		} else {

			mongoClient = new MongoClient(this.getHost());
		}

		// open DB

		DB db = mongoClient.getDB(dbName);

		// open store

		JSONStore jsonStore;

		jsonStore = new MongoDBJSONStore(mongoClient, db);
		jsonStore.init();

		// done

		return jsonStore;
	}

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
}
