package xdi2.core.impl.json.mongodb;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
	protected JsonObject loadInternal(String id) throws IOException {

		DBObject object = this.dbCollection.findOne(new BasicDBObject("_id", id));
		if (object == null) return new JsonObject();

		JsonObject jsonObject = fromMongoObject(object);

		return jsonObject;
	}


	@Override
	protected void saveInternal(String id, JsonObject jsonObject) throws IOException {

		DBObject object = toMongoObject(jsonObject, id);

		this.dbCollection.save(object);
	}

	@Override
	protected void deleteInternal(final String id) throws IOException {

		DBCursor cursor = this.dbCollection.find(new BasicDBObject("_id", "/" + escapeRegex(id) + ".*/"));

		while (cursor.hasNext()) {

			cursor.remove();
			cursor.next();
		}
	}

	/*
	 * Helper methods
	 */

	private static DBObject toMongoObject(JsonObject jsonObject, String id) {

		DBObject object = new BasicDBObject();

		for (Entry<String, JsonElement> entry : jsonObject.entrySet()) {

			String key = entry.getKey();
			key = key.replace("$", "\\$");

			object.put(key, JSON.parse(gson.toJson(entry.getValue())));
		}

		object.put("_id", id);

		return object;
	}

	private static JsonObject fromMongoObject(DBObject object) throws IOException {

		JsonObject jsonObject = new JsonObject();

		for (String key : object.keySet()) {

			if (key.equals("_id")) continue;
			
			StringBuilder builder = new StringBuilder();
			JSON.serialize(object.get(key), builder);

			key = key.replace("\\$", "$");

			JsonArray jsonArray = gson.getAdapter(JsonArray.class).fromJson("[" + builder.toString() + "]");

			jsonObject.add(key, jsonArray.get(0));
		}

		return jsonObject;
	}

	private static String escapeRegex(String string) {

		return string
				.replace("+", "\\+")
				.replace("=", "\\=")
				.replace("@", "\\@")
				.replace("$", "\\$")
				.replace("!", "\\!")
				.replace("*", "\\*")
				.replace("(", "\\(")
				.replace(")", "\\)");
	}

	public static void cleanup(String host) {

		cleanup(host, null);
	}

	public static void cleanup(String host, Integer port) {

		try {

			MongoClient mongoClient = port == null ? new MongoClient(host) : new MongoClient(host, port.intValue());

			List<String> databaseNames = mongoClient.getDatabaseNames();
			for (String databaseName : databaseNames) mongoClient.dropDatabase(databaseName);
		} catch (Exception ex) {

			throw new RuntimeException(ex.getMessage(), ex);
		}
	}
}
