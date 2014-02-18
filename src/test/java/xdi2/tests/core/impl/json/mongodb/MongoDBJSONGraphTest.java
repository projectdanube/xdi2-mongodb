package xdi2.tests.core.impl.json.mongodb;

import java.io.IOException;

import xdi2.core.Graph;
import xdi2.core.impl.json.mongodb.MongoDBJSONGraphFactory;
import xdi2.core.impl.json.mongodb.MongoDBJSONStore;
import xdi2.core.impl.json.mongodb.MongoDBStore;
import xdi2.tests.core.impl.AbstractGraphTest;

public class MongoDBJSONGraphTest extends AbstractGraphTest {

	private static MongoDBJSONGraphFactory graphFactory = new MongoDBJSONGraphFactory();

	public static final String HOST = "localhost";

	static {
		
		graphFactory.setHost(HOST);
		graphFactory.setMockFlag(Boolean.TRUE);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();

		MongoDBStore.cleanup(HOST, null, Boolean.TRUE);
	}

	@Override
	protected void tearDown() throws Exception {

		super.tearDown();

		MongoDBStore.cleanup(HOST, null, Boolean.TRUE);
	}

	@Override
	protected Graph openNewGraph(String identifier) throws IOException {

		return graphFactory.openGraph(identifier);
	}

	@Override
	protected Graph reopenGraph(Graph graph, String identifier) throws IOException {

		graph.close();

		return graphFactory.openGraph(identifier);
	}
}
