package milindparikh.diskstorage.accumulo.java.javapool;

import org.apache.commons.pool2.impl.GenericObjectPool;

public class AccumuloConnectorPool extends GenericObjectPool <AccumuloConnector> {
    public AccumuloConnectorPool(   AccumuloConnectorFactory  factory) {
	super(factory);
    }


}
