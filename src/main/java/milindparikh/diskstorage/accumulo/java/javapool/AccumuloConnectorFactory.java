package milindparikh.diskstorage.accumulo.java.javapool;


import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

public class AccumuloConnectorFactory     
    extends BasePooledObjectFactory<AccumuloConnector> {
    String [] args;
    

    public AccumuloConnectorFactory  (String[] args) {
	this.args = args;
    }
    
    @Override
    public AccumuloConnector create() {	
	AccumuloConnector ac = 
	    new AccumuloConnector();
	ac.parseArgs(AccumuloConnectorFactory.class.getName(), args);


	
	return ac;
	
    }

    /**
     * Use the default PooledObject implementation.
     */
    @Override
    public PooledObject<AccumuloConnector> wrap(AccumuloConnector accumuloConnector) {
        return new DefaultPooledObject<AccumuloConnector>(accumuloConnector);
    }

    /**
     * When an object is returned to the pool, clear the buffer.
     */
    /*
    @Override
    public void passivateObject(PooledObject<StringBuffer> pooledObject) {
        pooledObject.getObject().setLength(0);
    }
    */

    // for all other methods, the no-op implementation
    // in BasePooledObjectFactory will suffice
}
