package milindparikh.diskstorage.accumulo.java;

import java.util.Iterator;
import java.util.Map.Entry;

import milindparikh.diskstorage.accumulo.java.javapool.AccumuloConnector;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;

public class AccumuloJavaOrderedKeyColumnValueStore implements
		OrderedKeyValueStore {
	private final AccumuloJavaStoreManager storeManager;
	private String tableName;
	private final String columnFamilyName = "titan-cf";
	private final String columnFamilyQualifier = "titan-cq";
	
	AccumuloJavaOrderedKeyColumnValueStore(String tableName,
			AccumuloJavaStoreManager storeManager) {
		this.storeManager = storeManager;
		this.tableName = tableName;

	}

	@Override
	public String getName() {
		return tableName;
	}

	@Override
	public void close() throws StorageException {
		// Do nothing. Really?

	}

	@Override
	public StaticBuffer get(StaticBuffer key, StoreTransaction txh)
			throws StorageException {

		AccumuloConnector accumuloConnector = null;

		try {
			
			accumuloConnector = storeManager.getAccumuloConnector();
			Connector connector = accumuloConnector.getConnector();
			Scanner scanner = connector.createScanner(tableName,
					accumuloConnector.auths);

			Key startK = null;
			if (key != null)
				startK = new Key(new Text(key.asByteBuffer().array()));
			Key end = null;

			scanner.setRange(new Range(startK, end));
			Iterator<Entry<Key, Value>> iter = scanner.iterator();

			if (iter.hasNext()) {
				Entry<Key, Value> entry = iter.next();
				return new StaticByteBuffer(entry.getValue().toString()
						.getBytes());
			} else {
				return null;
			}
		} catch (AccumuloSecurityException ase) {
			throw new RuntimeException("Accumulo  Security exception"
					+ ase.toString());
		}

		catch (AccumuloException ae) {
			throw new RuntimeException("Some accumulo exception"
					+ ae.toString());
		} catch (TableNotFoundException te) {
			throw new RuntimeException("NO such table " + te.toString());
		} finally {
			storeManager.returnAccumuloConnector(accumuloConnector);
		}
	}

	@Override
	public boolean containsKey(StaticBuffer key, StoreTransaction txh)
			throws StorageException {

		boolean retValue = false;
		AccumuloConnector accumuloConnector = null;

		try {
			
			accumuloConnector = storeManager.getAccumuloConnector();
			Connector connector = accumuloConnector.getConnector();
			Scanner scanner = connector.createScanner(tableName,
					accumuloConnector.auths);

			Key startK = null;
			if (key != null)
				startK = new Key(new Text(key.asByteBuffer().array()));
			Key end = null;

			scanner.setRange(new Range(startK, end));
			Iterator<Entry<Key, Value>> iter = scanner.iterator();
			if (iter.hasNext()) {
				retValue = true;
			} else {
				retValue = false;
			}
		} catch (AccumuloSecurityException ase) {
			throw new RuntimeException("Accumulo  Security exception"
					+ ase.toString());
		}

		catch (AccumuloException ae) {
			throw new RuntimeException("Some accumulo exception"
					+ ae.toString());
		} catch (TableNotFoundException te) {
			throw new RuntimeException("NO such table " + te.toString());
		}

		storeManager.returnAccumuloConnector(accumuloConnector);
		return retValue;
	}

	@Override
	public void acquireLock(StaticBuffer key, StaticBuffer expectedValue,
			StoreTransaction txh) throws StorageException {
		// NO LOCKING IS REQD because features.supportsLocking = false
		throw new UnsupportedOperationException();
	}

	@Override
	public StaticBuffer[] getLocalKeyPartition() throws StorageException {
		// no support for localkeypartiton because features.hasLocalKeyPartition
		// = false
		throw new UnsupportedOperationException();
	}

	@Override
	public RecordIterator<KeyValueEntry> getSlice(StaticBuffer keyStart,
			StaticBuffer keyEnd, final KeySelector selector,
			StoreTransaction txh) throws StorageException {

		AccumuloConnector accumuloConnector = null;

		try {

			String startKey = new String(keyStart.asByteBuffer().array());
			String endKey = new String(keyEnd.asByteBuffer().array());

			accumuloConnector = storeManager.getAccumuloConnector();
			Connector connector = accumuloConnector.getConnector();
			Scanner scanner = connector.createScanner(tableName,
					accumuloConnector.auths);

			Key startK = null;
			if (startKey != null)
				startK = new Key(new Text(keyStart.asByteBuffer().array()));
			Key endK = null;
			if (endKey != null)
				endK = new Key(new Text(keyEnd.asByteBuffer().array()));

			scanner.setRange(new Range(startK, endK));
			final Iterator<Entry<Key, Value>> result = scanner.iterator();

			return new RecordIterator<KeyValueEntry>() {

				boolean reachedLimit = false;
				StaticBuffer key;
				StaticBuffer value;
				KeyValueEntry kve;
				boolean doValidate = true;

				@Override
				public boolean hasNext() {
					if (doValidate) {
						if (reachedLimit)
							return false;
						while (result.hasNext()) {
							if (reachedLimit)
								return false;

							Entry<Key, Value> entry = result.next();
							
							key = new StaticByteBuffer(entry.getKey()
									.getRow().getBytes());
							value = new StaticByteBuffer(entry.getValue().get());
							
					if (selector.include(key)) {
								reachedLimit = selector.reachedLimit();
								kve = new KeyValueEntry(key, value);
								doValidate = false;
								return true;
							}
						}
						return false;
					} else {
						return true;
					}

				}

				@Override
				public KeyValueEntry next() {
					if(kve == null){
						hasNext();
					}
					doValidate = true;
					return kve;
				}

				@Override
				public void close() {
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
		} catch (AccumuloSecurityException ase) {
			throw new RuntimeException("Accumulo  Security exception"
					+ ase.toString());
		} catch (AccumuloException ae) {
			throw new RuntimeException("Some accumulo exception"
					+ ae.toString());
		} catch (TableNotFoundException te) {
			throw new RuntimeException("NO such table " + te.toString());
		} finally {
			storeManager.returnAccumuloConnector(accumuloConnector);
		}
	}

	@Override
	public void insert(StaticBuffer key, StaticBuffer value,
			StoreTransaction txh) throws StorageException {
		AccumuloConnector accumuloConnector = null;

		try {

			accumuloConnector = storeManager.getAccumuloConnector();
			Connector connector = accumuloConnector.getConnector();

			BatchWriter writer = connector.createBatchWriter(tableName,
					new BatchWriterConfig());
			
			
			ColumnVisibility cv = new ColumnVisibility();
			Text cf = new Text(columnFamilyName);
			Text cq = new Text(columnFamilyQualifier);
			Mutation m = new Mutation(new Text(key.asByteBuffer().array()));
			m.put(cf, cq, cv, new Value(value.asByteBuffer().array()));
			// new Value(startValue.getBytes(Charset.forName("UTF-8"))));
			writer.addMutation(m);
			writer.close();

		} catch (AccumuloSecurityException ase) {
			throw new RuntimeException("Accumulo  Security exception"
					+ ase.toString());
		}

		catch (AccumuloException ae) {
			throw new RuntimeException("Some accumulo exception"
					+ ae.toString());
		} catch (TableNotFoundException te) {
			throw new RuntimeException("NO such table " + te.toString());
		}

		storeManager.returnAccumuloConnector(accumuloConnector);

	}

	@Override
	public void delete(StaticBuffer key, StoreTransaction txh)
			throws StorageException {
		AccumuloConnector accumuloConnector = null;

		try {

			accumuloConnector = storeManager.getAccumuloConnector();
			Connector connector = accumuloConnector.getConnector();

			BatchWriter writer = connector.createBatchWriter(tableName,
					new BatchWriterConfig());

			ColumnVisibility cv = new ColumnVisibility();
			Text cf = new Text(columnFamilyName);
			Text cq = new Text(columnFamilyQualifier);

			Mutation m = new Mutation(new Text(key.asByteBuffer().array()));
			m.putDelete(cf, cq, cv);
			writer.addMutation(m);
			writer.close();

		} catch (AccumuloSecurityException ase) {
			throw new RuntimeException("Accumulo  Security exception"
					+ ase.toString());
		}

		catch (AccumuloException ae) {
			throw new RuntimeException("Some accumulo exception"
					+ ae.toString());
		} catch (TableNotFoundException te) {
			throw new RuntimeException("NO such table " + te.toString());
		}

		storeManager.returnAccumuloConnector(accumuloConnector);

	}
}
