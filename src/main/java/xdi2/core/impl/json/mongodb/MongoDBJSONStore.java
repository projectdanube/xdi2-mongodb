package xdi2.core.impl.json.mongodb;


import java.io.IOException;
import java.util.List;

import xdi2.core.impl.json.AbstractJSONStore;
import xdi2.core.impl.json.JSONStore;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

public class MongoDBJSONStore extends AbstractJSONStore implements JSONStore {

	private static final Gson gson = new GsonBuilder().disableHtmlEscaping().create();

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

		StringBuilder builder = new StringBuilder();
		JSON.serialize(object, builder);
		System.err.println(builder.toString());
		JsonObject jsonObject = gson.getAdapter(JsonObject.class).fromJson(builder.toString());

		return jsonObject;
	}


	@Override
	protected void saveInternal(String id, JsonObject jsonObject) throws IOException {

		DBObject object = (DBObject) JSON.parse(gson.toJson(jsonObject));
		object.put("_id", id);

		this.dbCollection.insert(object);
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
