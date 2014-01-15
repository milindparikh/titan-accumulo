package milindparikh;


import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;




import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

/**
 * Hello world!
 *
 */


public class App 
{
	
    static TitanGraph graph;
	
    
    public static void main( String[] args )
    {
        
 	System.out.println( "Hello World!" );
        
	configureGraph();
	createNodesAndEdges();

        
    }
    

    static void createNodesAndEdges(){

    	System.out.println("Creating Nodes and Edges...");

    	Vertex juno = graph.addVertex(null);

    	juno.setProperty("name", "juno");

	/*
    	Vertex jupiter = graph.addVertex(null);

    	jupiter.setProperty("name", "jupiter");

    	Edge married = graph.addEdge(null, juno, jupiter, "married");

    	Vertex turnus = graph.addVertex(null);

    	turnus.setProperty("name", "turnus");

    	Vertex hercules = graph.addVertex(null);

    	hercules.setProperty("name", "hercules");

    	Vertex dido = graph.addVertex(null);

    	dido.setProperty("name", "dido");

    	Vertex troy = graph.addVertex(null);

    	troy.setProperty("name", "troy");
    	
    	Edge edge = graph.addEdge(null, juno, turnus, "knows");
    	edge.setProperty("since",2010);
    	edge.setProperty("stars",5);
    	edge = graph.addEdge(null, juno, hercules, "knows");
    	edge.setProperty("since",2011);
    	edge.setProperty("stars",1);
    	edge = graph.addEdge(null, juno, dido, "knows");
    	edge.setProperty("since", 2011);
    	edge.setProperty("stars", 5);
    	graph.addEdge(null, juno, troy, "likes").setProperty("stars",5);


	*/

    	graph.commit();
    	System.out.println("Created Nodes and Edges...");
    }
    
    static void retrieveNodes(){
    	System.out.println("Retrieving Nodes");
    	for(Vertex vertex: graph.getVertices()){
    		System.out.println("Vertex:" + vertex.getProperty("name"));

    	}
    	System.out.println("Retrieved Nodes");
    }



    static void configureToyGraph(){
    	 
	System.out.println("Configuring Toy Graph...");

         graph = TitanFactory.open("/tmp/titan");

         System.out.println("Done Configure Toy Graph....");

    }

    
    static void configureGraph(){
    	 
	System.out.println("Configuring Graph...");

    	 Configuration conf = new BaseConfiguration();

         conf.setProperty("storage.backend","milindparikh.diskstorage.accumulo.java.AccumuloJavaStoreManager");

         conf.setProperty("storage.instance","test");
         conf.setProperty("storage.user","root");
         conf.setProperty("storage.password","root");
         conf.setProperty("ids.password","root");



         graph = TitanFactory.open(conf);

         System.out.println("Configure Graph....");

    }
    
    
}
