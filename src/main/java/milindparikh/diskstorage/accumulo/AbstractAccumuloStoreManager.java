package milindparikh.diskstorage.accumulo;

import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractAccumuloStoreManager extends
		DistributedStoreManager implements OrderedKeyValueStoreManager {

	public static final String INSTANCE_KEY = "instance";
	public static final String INSTANCE_DEFAULT = "titan";
	protected final String instanceName;

	public static final String ZOOKEEPERS_KEY = "zookeepers";
	public static final String ZOOKEEPERS_DEFAULT = "localhost:2181";
	protected final String zookeeperNames;

	public static final String USER_KEY = "user";
	public static final String USER_DEFAULT = "user";
	protected final String user;

	public static final String PASSWORD_KEY = "password";
	public static final String PASSWORD_DEFAULT = "password";
	protected final String password;

	private StoreFeatures features = null;

	public AbstractAccumuloStoreManager(Configuration storageConfig) {
		super(storageConfig, 0);

		this.instanceName = storageConfig.getString(INSTANCE_KEY,
				INSTANCE_DEFAULT);
		this.zookeeperNames = storageConfig.getString(ZOOKEEPERS_KEY,
				ZOOKEEPERS_DEFAULT);
		this.user = storageConfig.getString(USER_KEY, USER_DEFAULT);
		this.password = storageConfig.getString(PASSWORD_KEY, PASSWORD_DEFAULT);

	}

	public String getName() {
		return getClass().getSimpleName();
	}

	@Override
	public StoreTransaction beginTransaction(final StoreTxConfig config) {
		return new AccumuloTransaction(config);
	}

	@Override
	public String toString() {
		return "[" + super.toString() + "]";
	}

	@Override
	public void mutateMany(Map<String, KVMutation> mutations,
			StoreTransaction txh) throws StorageException {
		throw new UnsupportedOperationException();
	}

	@Override
	public StoreFeatures getFeatures() {
		if (features == null) {
			features = new StoreFeatures();
			features.supportsOrderedScan = true;
			features.supportsUnorderedScan = false;
			features.supportsBatchMutation = false; // should revisit
			features.supportsTransactions = false;

			// ** what is this
			features.supportsConsistentKeyOperations = false;

			features.supportsLocking = false;
			features.isKeyOrdered = true;
			features.isDistributed = true;
			features.hasLocalKeyPartition = false;
			features.supportsMultiQuery = false;

		}
		return features;
	}

	@Override
	public void close() {

	}

	@Override
	public void clearStorage() throws StorageException {
		throw new UnsupportedOperationException();
	}

}
