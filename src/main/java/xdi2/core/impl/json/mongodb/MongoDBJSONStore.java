package xdi2.core.impl.json.mongodb;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
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

		return new BasicDBObject(MongoDBStore.XDI2_OBJ_KEY, key).append(MongoDBStore.XDI2_OBJ_ID, this.identifier);
	}

	@Override
	public JsonObject load(String id) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("load() - " + this.identifier + " " + id);
		}

		DBObject object = this.dbStore.getCollection().findOne(this.getKey(id));

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

		DBCursor cursor = this.dbStore.getCollection().find(this.getKey(toMongoStartsWithRegex(id)));
		if (cursor == null) return Collections.emptyMap();

		Map<String, JsonObject> jsonObjects = new HashMap<String, JsonObject> ();

		while (cursor.hasNext()) {

			DBObject object = cursor.next();

			jsonObjects.put((String) object.get(MongoDBStore.XDI2_OBJ_KEY), fromMongoObject(object));
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

		this.dbStore.getCollection().save(object);
	}

	@Override
	public void saveToArray(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveToArray() - " + this.identifier + " " + id + " " + key + " " + jsonPrimitive);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$addToSet", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	public void saveToObject(String id, String key, JsonElement jsonElement) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("saveToObject() - " + this.identifier + " " + id + " " + key + " " + jsonElement);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$set", new BasicDBObject(toMongoKey(key), toMongoElement(jsonElement))), true, false);
	}

	@Override
	public void delete(final String id) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("delete() - " + this.identifier + " " + id);
		}

		this.dbStore.getCollection().remove(this.getKey(toMongoStartsWithRegex(id)));
	}

	@Override
	public void deleteFromArray(String id, String key, JsonPrimitive jsonPrimitive) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteFromArray() - " + this.identifier + " " + id + " " + key + " " + jsonPrimitive);
		}

		this.dbStore.getCollection().update(this.getKey(id), new BasicDBObject("$pull", new BasicDBObject(toMongoKey(key), toMongoElement(jsonPrimitive))), true, false);
	}

	@Override
	public void deleteFromObject(String id, String key) throws IOException {

		if (log.isTraceEnabled()) {
			log.trace("deleteFromObject() - " + this.identifier + " " + id + " " + key);
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
