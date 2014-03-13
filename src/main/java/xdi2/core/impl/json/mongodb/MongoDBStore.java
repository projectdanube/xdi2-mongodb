package xdi2.core.impl.json.mongodb;

import java.util.List;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.BasicDBObject;

/**
 * This <code>MongoDBStore</code> defines MongoDB related objects used
 * for processing XDI2 graph objects.
 */
public class MongoDBStore {

	private static final Logger log = LoggerFactory.getLogger(MongoDBStore.class);

	public  static final String XDI2_OBJ_ID       = "id";
	public  static final String XDI2_OBJ_KEY      = "key";
	public  static final String XDI2_OBJ_INDEX    = "idx";

	public  static final String XDI2_DBNAME       = "xdi2graph";
	public  static final String XDI2_DBNAME_MOCK  = "xdi2graph_mock";
	public  static final String XDI2_DBCOLLECTION = "contexts";

	private MongoClient		client;
	private DB			db;
	private DBCollection		dbCollection;

	private static HashMap<String, MongoDBStore> mongoDBStores = new HashMap<String, MongoDBStore>();

	private MongoDBStore(MongoClient client, DB db, DBCollection dbCollection) {
		this.client = client;
		this.db = db;
		this.dbCollection = dbCollection;
	}

	public MongoClient getClient() {
		return this.client;
	}

	public DB getDB() {
		return this.db;
	}

	public DBCollection getCollection() {
		return this.dbCollection;
	}

	private static String getKey(String host, Integer port) {
		String key = host;
		if (port != null) {
			key = key + ":" + port;
		}
		return key;
	}

	public static synchronized MongoDBStore getMongoDBStore(String host, Integer port, Boolean mockFlag) {
		if (log.isTraceEnabled()) {
			log.trace("getMongoDBStore() " + host + " " + port + " " + mockFlag);
		}
		String key = getKey(host, port);
		MongoDBStore rtn = mongoDBStores.get(key);
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
			DB db = null;
			if (Boolean.TRUE.equals(mockFlag)) {
				db = client.getDB(XDI2_DBNAME_MOCK);
			} else {
				db = client.getDB(XDI2_DBNAME);
			}
			DBCollection dbCollection = db.getCollection(XDI2_DBCOLLECTION);
			BasicDBObject idx = new BasicDBObject();
			idx.put(XDI2_OBJ_KEY, Integer.valueOf(1));
			idx.put(XDI2_OBJ_ID , Integer.valueOf(1));
			dbCollection.ensureIndex(idx, XDI2_OBJ_INDEX, true);
			rtn = new MongoDBStore(client, db, dbCollection);
			mongoDBStores.put(key, rtn);
		} catch (java.net.UnknownHostException e) {
			log.error("getMongoDBStore() " + host + " " + port + " " + mockFlag + " failed - " + e, e);
			rtn = null;
		}
		return rtn;
	}

	public static void cleanup() {

		cleanup(null, null, Boolean.FALSE);
	}

	public static void cleanup(String host) {

		cleanup(host, null, Boolean.FALSE);
	}

	public static void cleanup(String host, Integer port, Boolean mockFlag) {

		String dbName = null;
		try {
			MongoDBStore dbStore = getMongoDBStore(host, port, mockFlag);
			MongoClient mongoClient = dbStore.getClient();
			List<String> databaseNames = mongoClient.getDatabaseNames();
			for (String databaseName : databaseNames) {
				dbName = databaseName;
				if (Boolean.TRUE.equals(mockFlag)) {
					if (XDI2_DBNAME_MOCK.equals(databaseName)) {
						if (log.isTraceEnabled()) {
							log.trace("cleanup() " + host + " " + port + " " + mockFlag + " db=" + databaseName);
						}
						mongoClient.dropDatabase(databaseName);
					}
				} else if(XDI2_DBNAME.equals(databaseName)) {
					if (log.isTraceEnabled()) {
						log.trace("cleanup ()" + host + " " + port + " " + mockFlag + " db=" + databaseName);
					}
					mongoClient.dropDatabase(databaseName);
				}
			}
		} catch (Exception ex) {
			log.error("cleanup() " + host + " " + port + " " + mockFlag + " db=" + dbName + " failed - " + ex, ex);
			throw new RuntimeException(ex.getMessage(), ex);
		}
	}
}
