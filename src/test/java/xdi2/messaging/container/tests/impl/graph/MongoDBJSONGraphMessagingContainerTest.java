package xdi2.messaging.container.tests.impl.graph;

import java.io.IOException;

import xdi2.core.Graph;
import xdi2.core.impl.json.mongodb.MongoDBJSONGraphFactory;
import xdi2.core.impl.json.mongodb.MongoDBJSONStore;
import xdi2.messaging.container.tests.impl.graph.AbstractGraphMessagingContainerTest;

public class MongoDBJSONGraphMessagingContainerTest extends AbstractGraphMessagingContainerTest {

	private static MongoDBJSONGraphFactory graphFactory = new MongoDBJSONGraphFactory();

	public static final String HOST = "localhost";

	static {
		graphFactory.setHost(HOST);
		graphFactory.setMockFlag(Boolean.TRUE);
	}

	@Override
	protected void setUp() throws Exception {

		super.setUp();
		MongoDBJSONStore.cleanup(HOST, null, Boolean.TRUE);
	}

	@Override
	protected void tearDown() throws Exception {

		super.tearDown();

		MongoDBJSONStore.cleanup(HOST, null, Boolean.TRUE);
	}

	@Override
	protected Graph openGraph(String identifier) throws IOException {

		return graphFactory.openGraph(identifier);
	}
}
