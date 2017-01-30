package testing;

import java.net.UnknownHostException;

import client.KVStore;
import app_kvServer.KVServer;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {

	public void testConnectionSuccess() {
		
		Exception ex = null;
		//new KVServer(6000, 10, "FIFO").start();
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}	
		
		assertNull(ex);
	}
	
	
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
	
	

	
}

