package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;


public class AllTests {

	static {
		try {
			Process p = Runtime.getRuntime().exec("rm -rf ./files/");
			p.destroy();
			new LogSetup("logs/testing/test.log", Level.ERROR);
			new KVServer(50000, 10, "FIFO");
		} catch (IOException e) {
			System.out.println("Deletion error?");
			e.printStackTrace();
		}
	}


	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);
		return clientSuite;
	}

}
