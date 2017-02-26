package app_kvServer;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;
import java.io.File;
import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

// Zookeeper stuff
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

/**
 * Represents a simple Echo Server implementation.
 */
public class KVServer extends Thread {

	private static Logger logger = Logger.getRootLogger();

	private int cacheSize;
	private String cacheStrategy;
    private ServerSocket serverSocket;
    private boolean running;
	static public Storage store = null;

	static public String ip;
    static public int port;
    static public String zNodePath;
    static public ZooKeeper zk = null;
    static private String beginRange;
    static private String endRange;
    static public String metadata;
    final CountDownLatch connectionLatch = new CountDownLatch(1);

    static private final Object keyRangeLock = new Object();

    
    static public String getBeginRange() {
        synchronized (keyRangeLock) {
            return KVServer.beginRange;
        }
    }
    static public void putBeginRange(String beginRange) {
        synchronized (keyRangeLock) {
            KVServer.beginRange = beginRange;
        }
    }
    static public String getEndRange() {
        synchronized (keyRangeLock) {
            return KVServer.endRange;
        }
    }
    static public void putEndRange(String endRange) {
        synchronized (keyRangeLock) {
            KVServer.endRange = endRange;
        }
    }
    static public String getMetadata() {
        synchronized (keyRangeLock) {
            return KVServer.metadata;
        }
    }
    static public void putMetadata(String metadata) {
        synchronized (keyRangeLock) {
            KVServer.metadata = metadata;
        }
    }

    /**
     * Constructs a (Echo-) Server object which listens to connection attempts
     * at the given port.
     *
     * @param port a port number which the Server is listening to in order to
     * 		establish a socket connection to a client. The port number should
     * 		reside in the range of dynamic ports, i.e 49152 - 65535.
     */
    public KVServer(int port, int cacheSize, String cacheStrategy, String ip){
        this.port = port;
		this.cacheSize = cacheSize;
		this.cacheStrategy = cacheStrategy;
        this.ip = ip;
		KVServer.store = new Storage(cacheSize, cacheStrategy);
        try {
            KVServer.zk = new ZooKeeper ("localhost", 2181, new Watcher() {

			    public void process(WatchedEvent we) {

				    if (we.getState() == KeeperState.SyncConnected) {
					    connectionLatch.countDown();
				    }
			    }
		    });

		    connectionLatch.await();

            this.zNodePath = "/" + this.ip + Integer.toString(port);
//            this.zk.create(this.zNodePath, "SERVER_STOPPED".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
//                    CreateMode.EPHEMERAL);

            String meta = getZNodeData("/Metadata", false);
            putMetadata(meta);
            String[] tokens = meta.split(";");
            for (int i = 0; i < tokens.length; i++)
            {
                String servStr = tokens[i];
                String[] tok = servStr.split(",");
                if (tok[0].equals(this.ip) && tok[1].equals(Integer.toString(this.port)))
                {
                    putBeginRange(tok[2]);
                    putEndRange(tok[3]);
                }
            } 

            
                
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.start();
    }

	public static Stat getZNodeStats(String path) throws KeeperException,
			InterruptedException {
		Stat stat = zk.exists(path, true);
		if (stat != null) {
			; //System.out.println("Node exists and the node version is " + path +
			//		+ stat.getVersion());
		} else {
			System.out.println("Node does not exists");
		}
		return stat;
	}


    public static String getZNodeData(String path, boolean watchFlag) throws KeeperException,
			InterruptedException {
		try {
			Stat stat = getZNodeStats(path);
			byte[] b = null;
			if (stat != null) {
                final CountDownLatch CLatch = new CountDownLatch(1);
				if(watchFlag){
//					Watcher watch = new Watcher();
					 b = zk.getData(path, new Watcher() {
			                public void process(WatchedEvent we) {
				            if (we.getState() == KeeperState.SyncConnected) {
					            CLatch.countDown();
				            }
			            }}, null);
				CLatch.await();
				}
                else
                {
					 b = zk.getData(path, null, null);
				}

				String data = new String(b, "UTF-8");
				//System.out.println(data);
				return data;
			} else {
				System.out.println("Node does not exists");
			}
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		return null;
    }


	public static void updateZNode(String path, byte[] data) throws KeeperException,
			InterruptedException {
		
        if(zk.exists(path, true) != null) {
            int version = KVServer.zk.exists(path, true).getVersion();
		    KVServer.zk.setData(path, data, version);
        }
        else {
            System.out.println("Node does not exist");
        }
	}

    /**
     * Initializes and starts the server.
     * Loops until the the server should be closed.
     */
    public void run() {

    	running = initializeServer();

        DataMigrator DataM = new DataMigrator();
        new Thread(DataM).start();

        System.out.println("Started Second thread");

        if(serverSocket != null) {
	        while(isRunning()){
	            try {
	                Socket client = serverSocket.accept();
	                ClientConnection connection =
	                		new ClientConnection(client, store);
	                new Thread(connection).start();

	                logger.info("Connected to "
	                		+ client.getInetAddress().getHostName()
	                		+  " on port " + client.getPort());
	            } catch (IOException e) {
	            	logger.error("Error! " +
	            			"Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
    }

    private boolean isRunning() {
        return this.running;
    }

    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void stopServer(){
        running = false;
        try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! " +
					"Unable to close socket on port: " + port, e);
		}
    }

    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	logger.info("Setting up files directory ...");
        System.out.println("ZNODEPATH : " + this.zNodePath);
        File filesdir = new File("/nfs/ug/homes-1/b/bettadpu/419/2/oldcode/files"+this.zNodePath);
        boolean result = false;

        if (!filesdir.exists()) {
            logger.info("Creating the files/ subdirectory ...");
        }

        try {
            result = filesdir.mkdir();
            for (File f : filesdir.listFiles()) {
                if (f.getName().endsWith(".lock"))
                    f.delete();
            }
            logger.info("Removed any old locks ...");
        }
        catch (Exception se) {
            logger.info("Failed to set up files/ subdirectory ...");
        }
        if (result) {
            logger.info("Finished Setting up files/ subdirectory ...");
        }
        else {
            logger.info("Failed to set up files/ subdirectory ...");
        }

    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: "
            		+ serverSocket.getLocalPort());
            return true;

        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }

    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			if(args.length != 4) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: <Server Port> <CacheSize> <Replacement Policy> <IP address>!");
			} else {
				int port = Integer.parseInt(args[0]);
                int cacheSize = Integer.parseInt(args[1]);
                String replacement = args[2];
                String ip = args[3];
    
    			new LogSetup("/nfs/ug/homes-1/b/bettadpu/419/2/oldcode/logs/server" + ip + args[0] + ".log", Level.ALL);
                if (replacement.equals("LRU") || replacement.equals("FIFO") || replacement.equals("LFU"))
    				new KVServer(port, cacheSize, replacement, ip);
                else {
                    System.out.println("Error! Cache Replacement must be LRU, LFU or FIFO");
                    System.exit(1);
                }
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
            nfe.printStackTrace();
			System.exit(1);
		}
    }
}
