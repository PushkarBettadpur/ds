package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

//import logging.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import common.messages.Message;

// Need to fix importing java.io.*
import java.io.*;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Collections;
import java.util.Map.Entry;

// The LRU + FIFO Cache. Apparently need to override
// the removeEldestEntry.
// https://discuss.leetcode.com/topic/54214/lfu-cache/14
class Cache extends LinkedHashMap<String, String> {
	private int cacheSize;

	public Cache(int size, boolean lru_or_fifo) {
		super(size, 0.75f, lru_or_fifo);
		this.cacheSize = size;
	}

	@Override
	protected boolean removeEldestEntry(
			java.util.Map.Entry<String,String> eldest) {
		// remove the oldest element when size limit is reached
		return size() > cacheSize;
	}


}
public class Storage {

	private String cacheStrategy;
	private int cacheSize;

    // lfu_kv can serve both LRU and FIFO
	private Cache lru_fC;
    // Need 2 hashmaps for LFU. One for key-frequency and
    // the other for key-value
	private Cache lfu_kv;
	private Map<String, Integer> lfu_kf;


	 public Storage(int cacheSize, String strategy) {
		this.cacheSize = cacheSize;
		this.cacheStrategy = strategy;
		this.lfu_kv = null;

        // Need 2 hashmaps for LFU. One for key-frequency and
        // the other for key-value
		this.lfu_kf = null;
		this.lru_fC = null;

		if (this.cacheStrategy == "LRU")
			lru_fC = new Cache(this.cacheSize, true);
		else if (this.cacheStrategy == "FIFO")
			lru_fC = new Cache(this.cacheSize, false);
		else {
			lfu_kf = new HashMap<String, Integer>();
			lfu_kv = new Cache(this.cacheSize, false);
		}
	}

    public String handleMessage(String latestMsg) {
        System.out.println("Received: " + latestMsg);
        String[] tokens = latestMsg.split(",");
        if(tokens[0].equals((Message.StatusType.GET).toString()) && tokens.length == 3) {

            System.out.println("GET_REQUEST<" + tokens[1] + ">");
            String key = tokens[1];

            try {
                Message result = getHelper(key);
                return result.toString();
            }
            catch (Exception e) {
                System.out.println("Exception in GET operation" + e);
                return (new Message(key, "null", Message.StatusType.GET_ERROR)).toString();
            }
        }
        else if(tokens[0].equals((Message.StatusType.PUT).toString()) && tokens.length == 3) {
            System.out.println("PUT_REQUEST<" + tokens[1] + "," + tokens[2] + ">");
            String key = tokens[1];
            String value = tokens[2];

            try {
                Message result = putHelper(key, value);
                return result.toString();
            }
            catch (Exception e) {
                System.out.println("Exception in PUT operation" + e);
                return (new Message(key, value, Message.StatusType.PUT_ERROR)).toString();
            }
        }
        else {
            System.out.println("Unknown Message Type");
            System.out.println(latestMsg);
        }
        return "Request Failed due to Unknown error";
    }


    public static <K, V> void printMap(Map<K, V> map) {
        for (Map.Entry<K, V> entry : map.entrySet()) {
            System.out.println("Key : " + entry.getKey()
                    + " Value : " + entry.getValue());
        }
    }

    // Debugging fn. Prints out the cache to stdout
    public void printCache()
    {
    	System.out.println(this.cacheStrategy);
    	if (this.cacheStrategy == "LRU" || this.cacheStrategy == "FIFO")
	    {
		    Set set = lru_fC.entrySet();
		    Iterator iterator = set.iterator();
		    while(iterator.hasNext()) {
			    Map.Entry me = (Map.Entry)iterator.next();
			    System.out.print("Key is: "+ me.getKey() + " & Value is: "+me.getValue()+"\n");
		    }
	    }
        else
        {
            System.out.println("Printing Kev Frequencies for LRU");
            printMap(lfu_kf);
            System.out.println("Printing Kev Values for LRU");
            printMap(lfu_kv);
        }
    	System.out.println("Printing Map is done \n\n");

    }

    private void removeFromCache(String key)
    {
	    if (this.cacheStrategy == "LRU" || this.cacheStrategy == "FIFO")
		    lru_fC.remove(key);
	    else
        {
		    lfu_kf.remove(key);
            lfu_kv.remove(key);
        }
    }

    // Does the key exist in the cache?
    private boolean findInCache(String key)
    {
	    if (this.cacheStrategy == "LRU" || this.cacheStrategy == "FIFO")
		    return lru_fC.containsKey(key);
	    else if (this.cacheStrategy == "LFU")
		    return lfu_kv.containsKey(key);
	    return false;
    }

    // Call this function only if you know the key
    // is in the cache. (call findInCache before)
    private String getValFromCache(String key)
    {
	    if (this.cacheStrategy == "LRU" || this.cacheStrategy == "FIFO")
		    return lru_fC.get(key);
	    else
        {
            // Increase the key-frequency by 1
		    lfu_kf.put(key, lfu_kf.get(key) + 1);
            return lfu_kv.get(key);
        }
    }

    private void insertIntoCache(String key, String value)
    {
        // I believe the HashLinkedMap already can remove the LRU
        // or FIFO element if it's exceeded it's capacity
	    if (this.cacheStrategy == "LRU" || this.cacheStrategy == "FIFO")
        {
		    lru_fC.put(key, value);
            return;
        }
	    else
	    {
            // if elem's already in cache, upgrade frequency by 1
            if (findInCache(key))
            {
                lfu_kf.put(key, lfu_kf.get(key) + 1);
                lfu_kv.put(key, value);
                return;
            }
            // If the cache is beyond it's capacity, then
            // We need to evict one.
		    else if (lfu_kv.size() >= this.cacheSize)
		    {
                // Find the smallest value in the Hashmap storing
                // the key-frequency pair. So smallest freq gets evicted
			    Entry<String, Integer> small = null;
                for (Entry<String, Integer> entry : lfu_kf.entrySet())
                {
                    if (small == null || small.getValue() > entry.getValue())
                        small = entry;
                }
                String toRemove = small.getKey();
                lfu_kf.remove(toRemove);
                lfu_kv.remove(toRemove);
                // Add new key and set its value to 1
                lfu_kf.put(key, 1);
                lfu_kv.put(key, value);
                return;
		    }
            // if there's no need to evict anything, add a new
            // element and set it's frequency to 1
            else
            {
                lfu_kf.put(key, 1);
                lfu_kv.put(key, value);
            }
            return;
	    }
    }

    // NOTE: Each Key is stored as a file and the file contents are
    // the value. This is done for fast I/O?
    // The strategy is to update the file AND the cache immediately.
    public Message putHelper(String key, String value) throws Exception
    {
	    String filename = "./files/"+key;

        String lockFileName = filename+".lock";
        File lockFile = new File(lockFileName);
        while (!lockFile.createNewFile());
        File varTmpDir = new File(filename);
        // Check if the file exists
	    boolean exists = varTmpDir.exists();

        // value = null => deletion
        if (value.equals("null"))
        {
            if (!exists)
            {
                lockFile.delete();//lock.release();
                return new Message(key, value, Message.StatusType.DELETE_ERROR);
            }
            else
            {
                removeFromCache(key);
                boolean rem = varTmpDir.delete();

                lockFile.delete();//lock.release();
                if (!rem)
                    return new Message(key, value, Message.StatusType.DELETE_ERROR);
                else
                    return new Message(key, value, Message.StatusType.DELETE_SUCCESS);
            }
        }

	    BufferedWriter bw = null;
	    FileWriter fw = null;
        try
        {
	        fw = new FileWriter(filename);
	        bw = new BufferedWriter(fw);
            // Update the file contents
	        bw.write(value);
	        // https://www.mkyong.com/java/how-to-write-to-file-in-java-bufferedwriter-example/
	        bw.close();
	        fw.close();
            // Insert into cache
	        insertIntoCache(key, value);

            lockFile.delete();//lock.release();
	        if (exists)
		        return new Message(key, value, Message.StatusType.PUT_UPDATE);
	        else
		        return new Message(key, value, Message.StatusType.PUT_SUCCESS);
        }
        catch (IOException e)
        {

            lockFile.delete();//lock.release();
		    System.out.println("Exception generated while opening/creating the file");
		    return new Message(key, value, Message.StatusType.PUT_ERROR);
        }
    }


    // NOTE: Each Key is stored as a file and the file contents are
    // the value. This is done for fast I/O?
    public Message getHelper(String key) throws Exception
    {
	    String filename = "./files/"+key;

        String lockFileName = filename+".lock";
        File lockFile = new File(lockFileName);
        while (!lockFile.createNewFile());
        // Check to see if the key can be served by the cache
	    boolean inCache = findInCache(key);
	    if (inCache)
	    {
		    String val = getValFromCache(key);
		    System.out.println("Cache val is "+val);

            lockFile.delete();//lock.release();
		    return new Message(key, val, Message.StatusType.GET_SUCCESS);
	    }

        File varTmpDir = new File(filename);

	    // Check if file exists
	    boolean exists = varTmpDir.exists();

	    BufferedReader br = null;
	    FileReader fr = null;
	    String value = null;

	    if (!exists) {

            lockFile.delete();//lock.release();
		    System.out.println("File doesn't exist "+filename);
		    return new Message(key, "null", Message.StatusType.GET_ERROR);
	    }

	    try
        {
		    fr = new FileReader(filename);
		    br = new BufferedReader(fr);
		    br = new BufferedReader(new FileReader(filename));
            // There's only 1 line? the value that is.
		    value = br.readLine();
		    if (value != null)
			    System.out.println("value is "+value);
		    else
			    System.out.println("value is NULL");
	    }

	    catch (IOException e) {

            lockFile.delete();//lock.release();
		    System.out.println("Exception thrown during DiskIO");
            return new Message(key, "null", Message.StatusType.GET_ERROR);
	    }

	    finally {
		    try
            {
			    if (br != null)
				    br.close();
			    if (fr != null)
				    fr.close();
		    }
		    catch (IOException e) {

                lockFile.delete();//lock.release();
			    System.out.println("Exception while closing file");
                return new Message(key, "null", Message.StatusType.GET_ERROR);
		    }

		    insertIntoCache(key, value);
            System.out.println("about to delete");
            lockFile.delete();//lock.release();
		    return new Message(key, value, Message.StatusType.GET_SUCCESS);
	    }
    }

    /*

    public static void main(String[] args) {
    	try {
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				KVServer serv = new KVServer(port, 5, "LRU");
				try {
                    // Random tests. Not very comprehensive
				    serv.putHelper("key1", "1");
				    serv.printCache();
				    serv.putHelper("key2", "2");
				    serv.printCache();
				    serv.putHelper("key3", "3");
				    serv.printCache();
				    serv.putHelper("key4", "4");
				    serv.printCache();
				    serv.putHelper("key5", "5");
				    serv.printCache();
				    serv.getHelper("key1");
				    serv.getHelper("key5");
                    serv.getHelper("key2");
                    serv.getHelper("key4");
				    serv.putHelper("key6", "6");
				    serv.putHelper("key3", "3");
 				    serv.printCache();
				}
				catch (Exception e) { }
			}
		}

/*        catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
*//*
        catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
    }
    */
}
