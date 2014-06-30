package xdi2.core.impl.json.mongodb.util;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import xdi2.core.impl.json.mongodb.MongoDBJSONGraphFactory;
import xdi2.core.impl.json.mongodb.MongoDBJSONStore;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * This <code>MongoDBStoreCombineUtil</code> class combines the existing
 * XDI2 graphs stored in its own MongoDB database into a single MongoDB database.
 */
public class MongoDBStoreCombineUtil
{
	/**
	 * The following are special XDI2 graph identifiers
	 */ 	 
	private static List<String> xdi2_special_ids;
	static
	{
		xdi2_special_ids = new ArrayList<String>();
		xdi2_special_ids.add("REGISTRY");
		xdi2_special_ids.add("OWNYOURINFO-REGISTRY");
		xdi2_special_ids.add("EMMETTGLOBAL-REGISTRY");
		xdi2_special_ids.add("FULLXRI-REGISTRY");
		xdi2_special_ids.add("TESTCSP-REGISTRY");
	}

	public static void addSpecialId(String id) {
		if (id != null) {
			xdi2_special_ids.add(id);
		}
	}

	private String      srcHost;
	private Integer     srcPort;
	private String      dstHost;
	private Integer     dstPort;
	private Boolean     useHash;
	private Boolean     dryRun;
	private MongoClient srcClient;
	private MongoClient dstClient;
	private int         cntUnknownIds;
	private Boolean     switchFlag;

	/**
	 * Constractor for instantiating a <code>MongoDBStoreCombineUtil</code> for
	 * copying XDI2 graphs stored in its own MongoDB database into a single
	 * MongoDB databases holding all XDI2 graphs.
	 *
	 * @param srcHost the host name of the source MongoDB instance.
	 * @param srcPort the port number of the source MongoDB instance.
	 * @param dstHost the host name of the target MongoDB instance.
	 * @param dstPort the port number of the target MongoDB instance.
	 * @param useHash the boolean flag indicating if hashed graph identifier.
	 *                should used in the target MongoDB instance.
	 * @param dryRun  the boolean flag for dry test run only.
	 */
	public MongoDBStoreCombineUtil(String srcHost, Integer srcPort, String dstHost, Integer dstPort, Boolean useHash, Boolean dryRun) {
		this.srcHost = srcHost;
		this.srcPort = srcPort;
		this.dstHost = dstHost;
		this.dstPort = dstPort;
		this.useHash = useHash;
		this.dryRun  = dryRun;
		this.srcClient = null;
		this.dstClient = null;
		this.cntUnknownIds = 0;
		this.switchFlag = Boolean.FALSE;
	}

	/**
	 * Sets the boolean flag indicating if [+] should be used instead of [@]
	 * for finding graph identifiers, with the default as FALSE, if not set
	 *
	 * @param switchFlag the boolean flag indicating if [+] should be used instead of [@]
	 */
	public void setSwitchFlag( Boolean switchFlag ) {
		this.switchFlag = switchFlag;
		if (this.switchFlag == null) {
			this.switchFlag = Boolean.FALSE;
		}
	}

	/**
	 * Gets the the boolean flag indicating if [+] should be used instead of [@]
	 * for finding graph identifiers, with the default as FALSE, if not set
	 *
	 * @return the boolean flag indicating if [+] should be used instead of [@], with
	 *         the default as FALSE
	 */
	public Boolean getSwitchFlag() {
		return this.switchFlag;
	}

	/**
	 * Gets an <code>MongoClient</code> object.
	 *
	 * @param host the host name of the MongoDB instance.
	 * @param post the port number of the MongoDB instance.
	 * @return a <code>MongoClient</code> object, or null if failure.
	 */
	private MongoClient getClient( String host, Integer port) {
		MongoClient rtn = null;
		try {
			if (port == null) {
				rtn = new MongoClient(host);
			} else {
				rtn = new MongoClient(host, port.intValue());
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
			rtn = null;
		}
		return rtn;
	}

	/**
	 * Initializes MongoDB connections.
	 *
	 * @return a boolean flag indicating if the operation is successful
	 */
	public boolean init() {
		this.srcClient = this.getClient(this.srcHost, this.srcPort);
		this.dstClient = this.getClient(this.dstHost, this.dstPort);
		if( (this.srcClient == null) || (this.dstClient == null)) {
			return false;
		}
		return true;
	}

	/**
	 * Closes MongoDB connections if needed.
	 */
	public void finish() {
		if (this.srcClient != null) {
			this.srcClient.close();
			this.srcClient = null;
		}
		if (this.dstClient != null) {
			this.dstClient.close();
			this.dstClient = null;
		}
	}

	/**
	 * Gets the hashed value of a XDI2 graph identifier
	 *
	 * @param identifier the XDI2 graph identifier in plaintext, usually a cloud number.
	 * @return the hash value of the identifier, or null if failure.
	 */
	private String hashIdentifier(String identifier) {
		String rtn = null;
		rtn = MongoDBJSONGraphFactory.hashIdentifier(identifier);
		if (identifier.equals(rtn)) {
			rtn = null;
		}
		return rtn;
	}

	/**
	 * Extracts XDI2 graph identifier from existing XDI2 graph object.
	 *
	 * @param db the database name of an existing XDI2 graph, which is the
	 *           hashed version of the XDI2 graph identifier.
	 *        obj the <code>DBObject</code> to be checked for XDI2 graph identifier value
	 * @return the XDI2 graph identifier in plaintext, or null if not found.
	 */
	private String getIdentifier(String db, DBObject obj) {
		String rtn = null;
		Object key = obj.get("_id");
		if (key == null) {
			return rtn;
		}
		if (! (key instanceof String)) {
			return rtn;
		}
		if (    ! "[=]".equals(key)
		     && (    (this.getSwitchFlag().equals(Boolean.FALSE) && ! "[@]".equals(key))
			  || (this.getSwitchFlag().equals(Boolean.TRUE ) && ! "[+]".equals(key)) ) ) {
			return rtn;
		}
		Object val = obj.get("");
		if (! (val instanceof BasicDBList)) {
			return rtn;
		}
		for ( Object item : (BasicDBList) val) {
			if (! (item instanceof String)) {
				continue;
			}
			String id = "(" + key + (String) item + ")";
			if (db.equals(this.hashIdentifier(id))) {
				rtn = id;
				break;
			}
		}
		if (rtn == null) {
			for (String id : xdi2_special_ids) {
				if (db.equals(this.hashIdentifier(id))) {
					rtn = id;
					break;
				}
			}
		}
		return rtn;
	}

	/**
	 * Copies an XDI2 graph from the existing MongoDB into the new one.
	 *
	 * @param db the MongoDB database name of the existing XDI2 graph.
	 * @param src the <code>DBCollection</code> object for the existing MongoDB database for the XDI2 graph.
	 * @param dst the <code>DBCollection</code> object for new MongoDB database.
	 * @return number of records copied.
	 */
	private int copy(String db, DBCollection src, DBCollection dst) {
		int rtn = 0;
		DBCursor cursor = src.find();
		try {
			List<DBObject> list = new ArrayList<DBObject>();
			String identifier = null;
			while (cursor.hasNext()) {
				DBObject obj = cursor.next();
				list.add(obj);
				if (identifier == null) {
					identifier = getIdentifier(db, obj);
					if( identifier != null ) {
						System.out.print("id " + identifier + " ");
					}
				}
			}
			if (identifier == null) {
				this.cntUnknownIds++;
			}
			if (Boolean.FALSE.equals(this.dryRun)) {
				if (Boolean.TRUE.equals(this.useHash)) {
					identifier = db;
				}
				if (identifier == null) {
					System.out.print("id NULL ");
				} else {
					for (DBObject obj : list) {
						Object key = obj.get("_id");
						obj.removeField("_id");
						obj.put(MongoDBJSONStore.XDI2_OBJ_KEY, key);
						obj.put(MongoDBJSONStore.XDI2_OBJ_ID , identifier);
						dst.insert(obj);
						rtn++;
					}
				}
			} else {
				rtn = list.size();
			}
			list.clear();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			cursor.close();
		}
	
		return rtn;
	}

	/**
	 * Copies all XDI2 graphs from the existing MongoDB databases into the new one.
	 *
	 * @return number of XDI2 graph copied.
	 */
	public void copy() {
		int totalGraphs  = 0;
		int totalRecords = 0;
		DBCollection dst = this.dstClient.getDB(MongoDBJSONStore.XDI2_DBNAME).getCollection(MongoDBJSONStore.XDI2_DBCOLLECTION);
		BasicDBObject idx = new BasicDBObject();
		idx.put(MongoDBJSONStore.XDI2_OBJ_KEY, Integer.valueOf(1));
		idx.put(MongoDBJSONStore.XDI2_OBJ_ID , Integer.valueOf(1));
		dst.ensureIndex(idx, MongoDBJSONStore.XDI2_OBJ_INDEX, true);
		long dstCount = dst.getCount();
		System.out.println("Old Records in Target: " + dstCount);

		int dbNameLength = this.hashIdentifier("([=]!:uuid:97ec0032-350f-4ccc-ab99-9ed09c1f994c)]").length();

		List<String> list = this.srcClient.getDatabaseNames();
		if (list != null) {
			for (String db : list) {
				if (db.length() != dbNameLength) {
					continue;
				}
				DBCollection src = this.srcClient.getDB(db).getCollection(MongoDBJSONStore.XDI2_DBCOLLECTION);
				System.out.print("Move graph " + totalGraphs + " " + db + " ... ");
				int n = this.copy(db, src, dst);
				totalGraphs++;
				totalRecords += n;
				System.out.println("done. " + n + " records copied.");
			}
		}
		System.out.println("Total Graphs: " + totalGraphs + " Total Records: " + totalRecords);
		System.out.println("Unknown Identifiers: " + this.cntUnknownIds + " New Records in Target: " + (dst.getCount() - dstCount));
	}

	/**
	 * Prints out the usage of this utility.
	 */
	private static void usage() {
		String name = MongoDBStoreCombineUtil.class.getName();
		System.out.println("Usage: java " + name + " [-sid specialid]* [-hash|-nohash] [-switch] [-test|-copy] -src sourcedb[:port] -dst targetdb[:port]");
		System.out.println("");
		System.out.println("Default: -nohash  does not use hashed values of graph identifiers in the target database"); 
		System.out.println("         -switch  use [+] instead of [@] for business names");
		System.out.println("         -test    does not perform actual insert operations in the target database");
		System.exit(1);
	}

	public static void main(String args[]) {
		Boolean useHash = Boolean.FALSE;
		Boolean dryRun  = Boolean.TRUE;
		String  srcHost = null;
		Integer srcPort = null;
		String  dstHost = null;
		Integer dstPort = null;
		Boolean switchC = null;
		int     i;
		for (i = 0; i < args.length; i++) {
			if ("-nohash".equals(args[i])) {
				useHash = Boolean.FALSE;
			} else if ("-hash".equals(args[i])) {
				useHash = Boolean.TRUE;
			} else if ("-switch".equals(args[i])) {
				switchC = Boolean.TRUE;
			} else if ("-copy".equals(args[i])) {
				dryRun = Boolean.FALSE;
			} else if ("-test".equals(args[i])) {
				dryRun = Boolean.TRUE;
			} else if ("-sid".equals(args[i]) && ((i + 1) < args.length)) {
				MongoDBStoreCombineUtil.addSpecialId(args[++i]);
			} else if ("-src".equals(args[i]) && ((i + 1) < args.length)) {
				srcHost = args[++i];
			} else if ("-dst".equals(args[i]) && ((i + 1) < args.length)) {
				dstHost = args[++i];
			} else {
				usage();
			}
		}
		if ((srcHost == null) || (dstHost == null)) {
			usage();
		}
		try
		{
			i = srcHost.indexOf(":");
			if (i > 0) {
				srcPort = Integer.valueOf(srcHost.substring(i + 1));
				srcHost = srcHost.substring(0, i);
			}
			i = dstHost.indexOf(":");
			if (i > 0) {
				dstPort = Integer.valueOf(dstHost.substring(i + 1));
				dstHost = dstHost.substring(0, i);
			}
		} catch (Exception e) {
			e.printStackTrace();
			usage();
		}
		MongoDBStoreCombineUtil util = new MongoDBStoreCombineUtil(srcHost, srcPort, dstHost, dstPort, useHash, dryRun);
		util.setSwitchFlag(switchC);
		try {
			if (util.init()) {
				util.copy();
			}
			util.finish();
		} catch (Exception ex) {
			ex.printStackTrace();
			System.exit(1);
		}
		System.exit(0);
	}
}
