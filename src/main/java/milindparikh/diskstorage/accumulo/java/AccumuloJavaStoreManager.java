package milindparikh.diskstorage.accumulo.java;

import java.util.HashMap;
import java.util.Map;

import milindparikh.diskstorage.accumulo.AbstractAccumuloStoreManager;
import milindparikh.diskstorage.accumulo.java.javapool.AccumuloConnector;
import milindparikh.diskstorage.accumulo.java.javapool.AccumuloConnectorFactory;
import milindparikh.diskstorage.accumulo.java.javapool.AccumuloConnectorPool;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.StorageException;

public class AccumuloJavaStoreManager extends AbstractAccumuloStoreManager {

	private final int MAX_PARAMS_IN_STORAGE_CONFIG = 5;

	private final Map<String, AccumuloJavaOrderedKeyColumnValueStore> openStores;
	private AccumuloConnectorPool pool;

	public AccumuloJavaStoreManager(Configuration storageConfig) {
		super(storageConfig);
		openStores = new HashMap<String, AccumuloJavaOrderedKeyColumnValueStore>(
				8);

		String args[] = new String[MAX_PARAMS_IN_STORAGE_CONFIG * 2];

		int i = 0;

		args[i] = "-i";
		i++;
		args[i] = storageConfig.getString("instance");
		i++;

		args[i] = "-u";
		i++;
		args[i] = storageConfig.getString("user");
		i++;

		args[i] = "-p";
		i++;
		args[i] = storageConfig.getString("password");
		i++;

		if (i < args.length) {
			while (i < args.length) {
				args[i] = "-" + new Integer(i);
				args[i + 1] = storageConfig.getString("user");
				i = i + 2;
			}

		}

		pool = new AccumuloConnectorPool(new AccumuloConnectorFactory(args));

	}

	@Override
	public synchronized AccumuloJavaOrderedKeyColumnValueStore openDatabase(
			String name) throws StorageException {
		if (openStores.containsKey(name))
			return openStores.get(name);
		else {
			ensureTableExists(name);
			AccumuloJavaOrderedKeyColumnValueStore store = new AccumuloJavaOrderedKeyColumnValueStore(
					name, this);
			openStores.put(name, store);
			return store;
		}
	}

	private void ensureTableExists(String name) {

		boolean retValue = false;
		AccumuloConnector accumuloConnector = null;

		try {

			accumuloConnector = getAccumuloConnector();
			Connector connector = accumuloConnector.getConnector();
			if (!(connector.tableOperations().exists(name))) {
				connector.tableOperations().create(name);
			}

		} catch (AccumuloSecurityException ase) {
			throw new RuntimeException("Accumulo  Security exception"
					+ ase.toString());
		}

		catch (AccumuloException ae) {
			throw new RuntimeException("Some accumulo exception"
					+ ae.toString());
		} catch (TableExistsException te) {
			throw new RuntimeException(
					"Table already exists-- THIS SHOULD NEVER OCCUR "
							+ te.toString());
		}

		returnAccumuloConnector(accumuloConnector);

	}

	public AccumuloConnector getAccumuloConnector() {
		AccumuloConnector accumuloConnector = null;

		try {
			accumuloConnector = pool.borrowObject();

		} catch (Exception e) {
			System.out.println(e.toString());

			throw new RuntimeException("Unable to borrow buffer from pool"
					+ e.toString());
		}
		return accumuloConnector;
	}

	public void returnAccumuloConnector(AccumuloConnector ac) {
		if (null != ac) {
			pool.returnObject(ac);
		}
	}

}
