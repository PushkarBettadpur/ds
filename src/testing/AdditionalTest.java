package testing;

import org.junit.Test;

import junit.framework.TestCase;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	private KVStore kvClient;
	private KVStore kvClient2;

	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		kvClient2 = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
            kvClient2.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
        kvClient2.disconnect();
	}
	
	@Test
	public void testsynchronization() {
		String key = "foo1";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "bar");
            response = kvClient2.put(key, "bar2");
            response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getValue().equals("bar2"));
	}

	@Test
	public void testSynchronization_2() {
		String key = "foo1";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "bar");
            response = kvClient2.get(key);
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getValue().equals("bar"));
	}

	@Test
	public void testCombination() {
		String key = "foo1";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "bar");
            response = kvClient2.put(key, "null");
            response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getStatus() == StatusType.GET_ERROR);
	}

	@Test
	public void testIncorrect() {
		String key = "foo111";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "null");
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	public void testGetAfterPut() {
		String key = "foo111";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "2");
            response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getValue().equals("2"));
	}
    
	@Test
	public void testDoubleDelete() {
		String key = "foo111";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "null");
            response = kvClient.put(key, "null");
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	public void testComplexCombinations() {
		String key = "foo111";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "3");
            response = kvClient.get(key);
            assertTrue(response.getStatus() == StatusType.GET_SUCCESS);
            response = kvClient.put(key, "null");
            assertTrue(response.getStatus() == StatusType.DELETE_SUCCESS);
            response = kvClient.get(key);
            assertTrue(response.getStatus() == StatusType.GET_ERROR);
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getStatus() == StatusType.DELETE_ERROR);
	}
   
	@Test
	public void testSpaces() {
		String key = "foo111";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, "My name is");
            response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && response.getValue().equals("My name is"));
	}

	@Test
	public void testPerformance() {
		KVMessage response = null;
		Exception ex = null;
        long elapsed = 0;
		try {
            long start = System.currentTimeMillis();
            
            for (int i = 0 ; i < 100; i++)
			    response = kvClient.put(Integer.toString(i), "random");

            elapsed = System.currentTimeMillis() - start;
            System.out.println("Time taken for consistent disk io is (ms) "+ Long.toString(elapsed));
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && elapsed < 10000);
	}

	@Test
	public void testCache() {
		KVMessage response = null;
		Exception ex = null;
        long elapsed = 0;
		try {
            long start = System.currentTimeMillis();
            kvClient.put(Integer.toString(120), "random");
            for (int i = 0 ; i < 100; i++)
			    response = kvClient.get(Integer.toString(120));

            elapsed = System.currentTimeMillis() - start;
            System.out.println("Time taken for consistent cache io is (ms) "+ Long.toString(elapsed));
		} catch (Exception e) {
			ex = e;
            System.out.println(ex);
		}

		assertTrue(ex == null && elapsed < 5000);
	}


}
