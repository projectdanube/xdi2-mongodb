package xdi2.tests.core.impl.json.mongodb;

import xdi2.core.GraphFactory;
import xdi2.core.impl.json.mongodb.MongoDBJSONGraphFactory;
import xdi2.core.impl.json.mongodb.MongoDBJSONStore;
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

		MongoDBJSONStore.cleanup(HOST, null, Boolean.TRUE);
	}

	@Override
	protected void tearDown() throws Exception {

		super.tearDown();

		MongoDBJSONStore.cleanup(HOST, null, Boolean.TRUE);
	}

	@Override
	protected GraphFactory getGraphFactory() {

		return graphFactory;
	}

	@Override
	protected boolean supportsPersistence() {

		return true;
	}
}
