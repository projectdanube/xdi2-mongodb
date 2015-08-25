package xdi2.core.impl.json.mongodb;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;

public class MongoDBJSONStore extends AbstractJSONStore implements JSONStore {

	private static final Logger log = LoggerFactory.getLogger(MongoDBJSONStore.class);
	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	public  static final String XDI2_OBJ_ID       = "id";
	public  static final String XDI2_OBJ_KEY      = "key";
	public  static final String XDI2_OBJ_INDEX    = "idx";

	public  static final String XDI2_DBNAME       = "xdi2graph";
	public  static final String XDI2_DBNAME_MOCK  = "xdi2graph_mock";
	public  static final String XDI2_DBCOLLECTION = "contexts";

	private MongoClient	mongoClient;
	private String		identifier;
	private Boolean mockFlag;
	private Boolean sharedDatabaseFlag;

	private DBCollection dbCollection;

	public MongoDBJSONStore(MongoClient mongoClient, String identifier, Boolean mockFlag, Boolean sharedDatabaseFlag) {

		this.mongoClient = mongoClient;
		this.identifier = identifier;
		this.mockFlag = mockFlag;
		this.sharedDatabaseFlag = sharedDatabaseFlag;
	}

	@Override
	public void init() {

		DB db = null;
		if (Boolean.TRUE.equals(mockFlag)) {
			db = this.mongoClient.getDB(XDI2_DBNAME_MOCK);
		} else {
			if (Boolean.TRUE.equals(sharedDatabaseFlag)) {
				db = this.mongoClient.getDB(XDI2_DBNAME);
			} else {
				db = this.mongoClient.getDB(this.identifier);
			}
		}
		this.dbCollection = db.getCollection(XDI2_DBCOLLECTION);
		if (Boolean.TRUE.equals(sharedDatabaseFlag)) {
			BasicDBObject idx = new BasicDBObject();
			idx.put(XDI2_OBJ_KEY, Integer.valueOf(1));
			idx.put(XDI2_OBJ_ID , Integer.valueOf(1));
			dbCollection.ensureIndex(idx, XDI2_OBJ_INDEX, true);
		}
	}

	@Override
	public void close() {}

	/**
	 * Constructs the search <code>BasicDBObject</code> by combiniing graph identifier and the secondary key.
	 *
	 * @param key the secondary key of a XDI2 graph.
	 * @return a <code>BasicDBObject</code> combiniing graph identifier and the secondary key.
	 */
	private BasicDBObject getKey(Object key) {

		if (Boolean.TRUE.equals(this.sharedDatabaseFlag)) {
			return new BasicDBObject(XDI2_OBJ_KEY, key).append(XDI2_OBJ_ID, this.identifier);
		} else {
			return new BasicDBObject("_id", key);
		}
	}

	@Override
	public JsonObject load(String id) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("load() - " + this.identifier + " " + id);
		}

		DBObject object = this.dbCollection.findOne(this.getKey(id));

		if (log.isTraceEnabled()) {
			log.trace("load() - " + this.identifier + " " + id + " = " + object);
		}

		JsonObject jsonObject = fromMongoObject(object);

		if (log.isTraceEnabled()) {
			log.trace("load() - " + this.identifier + " " + id + " = " + jsonObject);
		}

		return jsonObject;
	}

	@Override
	public Map<String, JsonObject> loadWithPrefix(String id) throws IOException {

		DBCursor cursor = this.dbCollection.find(this.getKey(toMongoStartsWithRegex(id)));
		if (cursor == null) return Collections.emptyMap();

		Map<String, JsonObject> jsonObjects = new HashMap<String, JsonObject> ();

		while (cursor.hasNext()) {

			DBObject object = cursor.next();

			jsonObjects.put((String) object.get(XDI2_OBJ_KEY), fromMongoObject(object));
		}

		cursor.close();

		return jsonObjects;
	}

	@Override
	public void save(String id, JsonObject jsonObject) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("save() - " + this.identifier + " " + id + " " + jsonObject);
		}

		DBObject object = toMongoObject(jsonObject, id);

		if (log.isTraceEnabled()) {
			log.trace("save() - " + this.identifier + " " + id + " " + object);
		}

		this.dbCollection.save(object);
	}

	@Override
	public void saveToArray(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveToArray() - " + this.identifier + " " + id + " " + key + " " + jsonPrimitive);
		}

		this.dbCollection.update(this.getKey(id), new BasicDBObject("$addToSet", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	public void saveToObject(String id, String key, JsonElement jsonElement) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveToObject() - " + this.identifier + " " + id + " " + key + " " + jsonElement);
		}

		this.dbCollection.update(this.getKey(id), new BasicDBObject("$set", new BasicDBObject(toMongoKey(key), toMongoElement(jsonElement))), true, false);
	}

	@Override
	public void delete(final String id) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("delete() - " + this.identifier + " " + id);
		}

		this.dbCollection.remove(this.getKey(toMongoStartsWithRegex(id)));
	}

	@Override
	public void deleteFromArray(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteFromArray() - " + this.identifier + " " + id + " " + key + " " + jsonPrimitive);
		}

		this.dbCollection.update(this.getKey(id), new BasicDBObject("$pull", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	public void deleteFromObject(String id, String key) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteFromObject() - " + this.identifier + " " + id + " " + key);
		}

		this.dbCollection.update(this.getKey(id), new BasicDBObject("$unset", new BasicDBObject(toMongoKey(key), "")), false, false);
	}

	/*
	 * Helper methods
	 */

	private DBObject toMongoObject(JsonObject jsonObject, String id) {

		DBObject object = this.getKey(id);
		for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {

			String key = entry.getKey();
			JsonElement value = entry.getValue();

			object.put(toMongoKey(key), toMongoElement(value));
		}

		return object;
	}

	private JsonObject fromMongoObject(DBObject object) throws IOException {

		if (object == null) {
			return null;
		}

		JsonObject jsonObject = new JsonObject();

		for (String key : object.keySet()) {
			if (key.equals(XDI2_OBJ_ID) || key.equals(XDI2_OBJ_KEY) || key.equals("_id")) {
				continue;
			}
			Object value = object.get(key);
			jsonObject.add(fromMongoKey(key), fromMongoElement(value));
		}

		return jsonObject;
	}

	private Object toMongoElement(JsonElement jsonElement) {

		return JSON.parse(gson.toJson(jsonElement));
	}

	private JsonElement fromMongoElement(Object object) throws IOException {

		StringBuilder builder = new StringBuilder();
		JSON.serialize(object, builder);

		return gson.getAdapter(JsonArray.class).fromJson("[" + builder.toString() + "]").get(0);
	}

	private String toMongoKey(String key) {

		if (key.startsWith("$")) key = "\\" + key;

		return key;
	}

	private String fromMongoKey(String key) {

		if (key.startsWith("\\$")) key = key.substring(1);

		return key;
	}

	private Pattern toMongoStartsWithRegex(String string) {

		StringBuilder buffer = new StringBuilder();

		buffer.append("^");

		buffer.append(string
				.replace("+", "\\+")
				.replace("=", "\\=")
				.replace("@", "\\@")
				.replace("$", "\\$")
				.replace("!", "\\!")
				.replace("*", "\\*")
				.replace("(", "\\(")
				.replace(")", "\\)")
				.replace("[", "\\[")
				.replace("]", "\\]")
				.replace("{", "\\{")
				.replace("}", "\\}")
				.replace("<", "\\<")
				.replace(">", "\\>")
				.replace("&", "\\&"));

		buffer.append(".*");

		return Pattern.compile(buffer.toString());
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
			MongoClient mongoClient = MongoDBJSONGraphFactory.getMongoClient(host, port);
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
