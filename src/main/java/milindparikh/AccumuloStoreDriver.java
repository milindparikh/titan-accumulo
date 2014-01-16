package milindparikh;

import java.nio.charset.Charset;

import milindparikh.diskstorage.accumulo.java.AccumuloJavaOrderedKeyColumnValueStore;
import milindparikh.diskstorage.accumulo.java.AccumuloJavaStoreManager;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

public class AccumuloStoreDriver {
	private static AccumuloJavaStoreManager accumuloJavaStoreManager = null;

	public static void main(String[] args) throws StorageException {

		BaseConfiguration storeConfig = new BaseConfiguration();

		storeConfig.addProperty("zookeepers", "localhost:2181");
		storeConfig.addProperty("instance", "test");
		storeConfig.addProperty("user", "root");
		storeConfig.addProperty("password", "root");

		accumuloJavaStoreManager = new AccumuloJavaStoreManager(storeConfig);
		testContainsKey();
		testInsert();
		testContainsKey();
		testDelete();
		testContainsKey();
		testInsert();
		testContainsKey();
		testGet();
		testGetSlice();

	}

	public static void testDelete() throws StorageException {
		AccumuloJavaOrderedKeyColumnValueStore accumuloJavaOrderedKeyColumnValueStore = accumuloJavaStoreManager
				.openDatabase("hellotable2");

		StoreTxConfig config = new StoreTxConfig();
		StoreTransaction st = accumuloJavaStoreManager.beginTransaction(config);
		accumuloJavaOrderedKeyColumnValueStore.delete(new StaticByteBuffer(
				"row9".getBytes()), st);

	}

	public static void testInsert() throws StorageException {

		AccumuloJavaOrderedKeyColumnValueStore accumuloJavaOrderedKeyColumnValueStore = accumuloJavaStoreManager
				.openDatabase("hellotable2");

		StoreTxConfig config = new StoreTxConfig();
		StoreTransaction st = accumuloJavaStoreManager.beginTransaction(config);

		accumuloJavaOrderedKeyColumnValueStore
				.insert(new StaticByteBuffer("row1".getBytes()),
						new StaticByteBuffer("val1".getBytes()), st);
		accumuloJavaOrderedKeyColumnValueStore
				.insert(new StaticByteBuffer("row8".getBytes()),
						new StaticByteBuffer("val1".getBytes()), st);
		accumuloJavaOrderedKeyColumnValueStore
				.insert(new StaticByteBuffer("row9".getBytes()),
						new StaticByteBuffer("val1".getBytes()), st);

	}

	public static void testGet() throws StorageException {

		AccumuloJavaOrderedKeyColumnValueStore accumuloJavaOrderedKeyColumnValueStore = accumuloJavaStoreManager
				.openDatabase("hellotable2");

		StoreTxConfig config = new StoreTxConfig();
		StoreTransaction st = accumuloJavaStoreManager.beginTransaction(config);

		StaticBuffer buf = accumuloJavaOrderedKeyColumnValueStore.get(
				new StaticByteBuffer("row9".getBytes()), st);
		String str = new String(buf.asByteBuffer().array(),
				Charset.forName("UTF-8"));
		System.out.println(str);

	}

	public static void testGetSlice() throws StorageException {

		AccumuloJavaOrderedKeyColumnValueStore accumuloJavaOrderedKeyColumnValueStore = accumuloJavaStoreManager
				.openDatabase("hellotable2");

		StoreTxConfig config = new StoreTxConfig();
		StoreTransaction st = accumuloJavaStoreManager.beginTransaction(config);

		StaticBuffer startB = new StaticByteBuffer("row1".getBytes());
		StaticBuffer endB = new StaticByteBuffer("row9".getBytes());

		final KeySelector selectAll = new KeySelector() {

			@Override
			public boolean include(StaticBuffer key) {
				return true;
			}

			@Override
			public boolean reachedLimit() {
				return false;
			}

		};

		RecordIterator<KeyValueEntry> iter = accumuloJavaOrderedKeyColumnValueStore
				.getSlice(startB, endB, selectAll, st);

		while (iter.hasNext()) {
			System.out.println(iter.next());
		}

	}

	public static void testContainsKey() throws StorageException {

		AccumuloJavaOrderedKeyColumnValueStore accumuloJavaOrderedKeyColumnValueStore = accumuloJavaStoreManager
				.openDatabase("hellotable2");

		StoreTxConfig config = new StoreTxConfig();
		StoreTransaction st = accumuloJavaStoreManager.beginTransaction(config);

		System.out.println(accumuloJavaOrderedKeyColumnValueStore.containsKey(
				new StaticByteBuffer("row9".getBytes()), st));

	}

}
