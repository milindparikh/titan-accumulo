package milindparikh.diskstorage.accumulo.java;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableExistsException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import milindparikh.diskstorage.accumulo.java.javapool.*;

import  milindparikh.diskstorage.accumulo.AbstractAccumuloStoreManager ;
import static milindparikh.diskstorage.accumulo.AccumuloTransaction.getTx;

public class AccumuloJavaStoreManager extends AbstractAccumuloStoreManager {

    private final int MAX_PARAMS_IN_STORAGE_CONFIG=5;
    
    private final Map<String, AccumuloJavaOrderedKeyColumnValueStore> openStores;
    private   AccumuloConnectorPool pool;

    
    
    public AccumuloJavaStoreManager(Configuration storageConfig) {
	super(storageConfig);
	openStores = new HashMap<String, AccumuloJavaOrderedKeyColumnValueStore>(8);

	
	String args[] = new String[MAX_PARAMS_IN_STORAGE_CONFIG*2];
	
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
		args[i] = "-"+new Integer(i);
		args[i+1] = storageConfig.getString("user");
		i=i+2;
	    }
	    
	}
	

	
	
	pool = 
	    new AccumuloConnectorPool 
	    (new AccumuloConnectorFactory(args));
    
    }

    @Override
    public synchronized AccumuloJavaOrderedKeyColumnValueStore openDatabase(String name) throws StorageException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
	    ensureTableExists(name);
            AccumuloJavaOrderedKeyColumnValueStore store = new AccumuloJavaOrderedKeyColumnValueStore(name, this);
            openStores.put(name, store);
            return store;
        }
    }

    private void ensureTableExists (String name) {
	
	
	boolean retValue = false;
	AccumuloConnector accumuloConnector = null;
	
	try {
	    
	    accumuloConnector = getAccumuloConnector();
	    Connector connector = accumuloConnector.getConnector();
	    if (! (connector.tableOperations().exists(name) ))  {
		connector.tableOperations().create(name);
	    }
	    
	}
	catch  (AccumuloSecurityException ase) {
	    throw new RuntimeException("Accumulo  Security exception" + ase.toString());
	}

	catch  (AccumuloException ae) {
	    throw new RuntimeException("Some accumulo exception" + ae.toString());
	}
	catch ( TableExistsException te) {
	    throw new RuntimeException("Table already exists-- THIS SHOULD NEVER OCCUR " + te.toString());
	}

	returnAccumuloConnector(accumuloConnector);
	
    }
    
    

    
    public  AccumuloConnector getAccumuloConnector()  {
	AccumuloConnector accumuloConnector = null;
	
	try {
	    accumuloConnector = pool.borrowObject();

	}
	catch (Exception e) {
	    System.out.println(e.toString());
	    
            throw new RuntimeException("Unable to borrow buffer from pool" + e.toString());
        }
	return accumuloConnector;
    }
    
    public void returnAccumuloConnector( AccumuloConnector ac) {
	if (null != ac) {
	    pool.returnObject(ac);
	}
    }
    

}
