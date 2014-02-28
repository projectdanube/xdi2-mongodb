package xdi2.core.impl.json.mongodb;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;

import xdi2.core.exceptions.Xdi2RuntimeException;
import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class MongoDBJSONStore extends AbstractJSONStore implements JSONStore {

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	private MongoClient mongoClient;
	private DB db;
	private DBCollection dbCollection;

	public MongoDBJSONStore(MongoClient mongoClient, DB db) {

		this.mongoClient = mongoClient;
		this.db = db;

		this.dbCollection = null;
	}

	@Override
	public void init() throws IOException {

		this.dbCollection = this.db.getCollection("contexts");
	}

	@Override
	public void close() {

		this.mongoClient.close();
	}

	@Override
	public JsonObject load(String id) throws IOException {

		DBObject object = this.dbCollection.findOne(new BasicDBObject("_id", id));
		if (object == null) return null;

		JsonObject jsonObject = fromMongoObject(object);

		return jsonObject;
	}

	@Override
	public void save(String id, JsonObject jsonObject) throws IOException {

		DBObject object = toMongoObject(jsonObject, id);

		this.dbCollection.save(object);
	}

	@Override
	public void saveToArray(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		this.dbCollection.update(new BasicDBObject("_id", id), new BasicDBObject("$addToSet", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	public void saveToObject(String id, String key, JsonElement jsonElement) throws IOException {

		this.dbCollection.update(new BasicDBObject("_id", id), new BasicDBObject("$set", new BasicDBObject(toMongoKey(key), toMongoElement(jsonElement))), true, false);
	}

	@Override
	public void delete(final String id) throws IOException {

		this.dbCollection.remove(new BasicDBObject("_id", toMongoStartsWithRegex(id)));
	}

	@Override
	public void deleteFromArray(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		this.dbCollection.update(new BasicDBObject("_id", id), new BasicDBObject("$pull", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	public void deleteFromObject(String id, String key) throws IOException {

		this.dbCollection.update(new BasicDBObject("_id", id), new BasicDBObject("$unset", new BasicDBObject(toMongoKey(key), "")), false, false);
	}

	/*
	 * Helper methods
	 */

	private static DBObject toMongoObject(JsonObject jsonObject, String id) {

		DBObject object = new BasicDBObject();

		for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {

			String key = entry.getKey();
			JsonElement value = entry.getValue();

			object.put(toMongoKey(key), toMongoElement(value));
		}

		object.put("_id", id);

		return object;
	}

	private static JsonObject fromMongoObject(DBObject object) throws IOException {

		JsonObject jsonObject = new JsonObject();

		for (String key : object.keySet()) {

			Object value = object.get(key);

			if (key.equals("_id")) continue;

			jsonObject.add(fromMongoKey(key), fromMongoElement(value));
		}

		return jsonObject;
	}

	private static Object toMongoElement(JsonElement jsonElement) {

		return JSON.parse(gson.toJson(jsonElement));
	}

	private static JsonElement fromMongoElement(Object object) throws IOException {

		StringBuilder builder = new StringBuilder();
		JSON.serialize(object, builder);

		return gson.getAdapter(JsonArray.class).fromJson("[" + builder.toString() + "]").get(0);
	}

	private static String toMongoKey(String key) {

		if (key.startsWith("$")) key = "\\" + key;

		return key;
	}

	private static String fromMongoKey(String key) {

		if (key.startsWith("\\$")) key = key.substring(1);

		return key;
	}

	static String toMongoDBName(String identifier) {

		try {

			MessageDigest digest = MessageDigest.getInstance("SHA-256");

			return new String(Base64.encodeBase64URLSafe(digest.digest(identifier.getBytes("UTF-8"))));
		} catch (Exception ex) {

			throw new Xdi2RuntimeException(ex);
		}
	}

	private static Pattern toMongoStartsWithRegex(String string) {

		StringBuilder buffer = new StringBuilder();

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

		cleanup(null, null);
	}

	public static void cleanup(String host) {

		cleanup(host, null);
	}

	public static void cleanup(String host, Integer port) {

		try {

			MongoClient mongoClient = port == null ? (host == null ? new MongoClient() : new MongoClient(host)) : new MongoClient(host, port.intValue());

			List<String> databaseNames = mongoClient.getDatabaseNames();
			for (String databaseName : databaseNames) mongoClient.dropDatabase(databaseName);
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage(), ex);
		}
	}
}
