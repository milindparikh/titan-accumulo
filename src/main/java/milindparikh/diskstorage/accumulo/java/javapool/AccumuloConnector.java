package milindparikh.diskstorage.accumulo.java.javapool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import org.apache.accumulo.core.cli.ClientOpts;
import org.apache.accumulo.core.cli.Help;



public class AccumuloConnector extends ClientOpts {

    public void parseArgs(String programName, String[] args, Object ... others) {
	JCommander commander = new JCommander();
	commander.setAcceptUnknownOptions(true);

	for (String s: args) {
	    System.out.println(s);
	}
	
	commander.addObject(this);
	for (Object other : others)
	    commander.addObject(other);
	commander.setProgramName(programName);
	try {
	    commander.parse(args);
	} catch (ParameterException ex) {
	    commander.usage();
	    exitWithError(ex.getMessage(), 1);
	}
	if (help) {
	    commander.usage();
	    exit(0);
	}
    }
  public void exit(int status) {
    System.exit(status);
  }
  
  public void exitWithError(String message, int status) {
    System.err.println(message);
    exit(status);
  }


}
