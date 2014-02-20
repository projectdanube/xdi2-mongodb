package xdi2.core.impl.json.mongodb;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

	private static final Logger log = LoggerFactory.getLogger(MongoDBJSONStore.class);
	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().serializeNulls().create();

	private MongoDBStore	dbStore;
	private String		identifier;

	public MongoDBJSONStore(MongoDBStore dbStore, String identifier) {

		this.dbStore = dbStore;
		this.identifier = identifier;
	}

	@Override
	public void init() {}

	@Override
	public void close() {}

	/**
	 * Constructs the search <code>BasicDBObject</code> by combiniing graph identifier and the secondary key.
	 *
	 * @param key the secondary key of a XDI2 graph.
	 * @return a <code>BasicDBObject</code> combiniing graph identifier and the secondary key.
	 */
	private BasicDBObject getKey(Object key) {

		return new BasicDBObject(MongoDBStore.XDI2_OBJ_KEY, this.identifier).append(MongoDBStore.XDI2_OBJ_ID, key);
	}

	@Override
	protected JsonObject loadInternal(String id) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("loadInternal() - " + this.identifier + " " + id);
		}

		DBObject object = this.dbStore.getCollection().findOne(this.getKey(id));

		if (log.isTraceEnabled()) {
			log.trace("loadInternal() - " + this.identifier + " " + id + " = " + object);
		}

		JsonObject jsonObject = fromMongoObject(object);

		if (log.isTraceEnabled()) {
			log.trace("loadInternal() - " + this.identifier + " " + id + " = " + jsonObject);
		}

		return jsonObject;
	}

	@Override
	protected void saveInternal(String id, JsonObject jsonObject) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveInternal() - " + this.identifier + " " + id + " " + jsonObject);
		}

		DBObject object = toMongoObject(jsonObject, id);

		if (log.isTraceEnabled()) {
			log.trace("saveInternal() - " + this.identifier + " " + id + " " + object);
		}

		this.dbStore.getCollection().save(object);
	}

	@Override
	protected void saveToArrayInternal(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveToArrayInternal() - " + this.identifier + " " + id + " " + key + " " + jsonPrimitive);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$addToSet", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	protected void saveToObjectInternal(String id, String key, JsonElement jsonElement) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveToObjectInternal() - " + this.identifier + " " + id + " " + key + " " + jsonElement);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$set", new BasicDBObject(toMongoKey(key), toMongoElement(jsonElement))), true, false);
	}

	@Override
	protected void deleteInternal(final String id) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteInternal() - " + this.identifier + " " + id);
		}

		this.dbStore.getCollection().remove(this.getKey(toMongoStartsWithRegex(id)));
	}

	@Override
	protected void deleteFromArrayInternal(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteFromArrayInternal() - " + this.identifier + " " + id + " " + key + " " + jsonPrimitive);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$pull", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	protected void deleteFromObjectInternal(String id, String key) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteFromObjectInternal() - " + this.identifier + " " + id + " " + key);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$unset", new BasicDBObject(toMongoKey(key), "")), false, false);
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
			if (key.equals(MongoDBStore.XDI2_OBJ_ID) || key.equals(MongoDBStore.XDI2_OBJ_KEY) || key.equals("_id")) {
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
}
