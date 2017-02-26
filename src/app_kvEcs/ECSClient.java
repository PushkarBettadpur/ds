package app_kvEcs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.io.File;
import java.io.FileReader;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import logger.LogSetup;

import app_kvClient.Client;
import app_kvClient.ClientSocketListener;
import app_kvClient.TextMessage;

import client.KVStore;
import common.messages.Message;

import java.util.concurrent.CountDownLatch;
// import zookeeper classes
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;


// MD5 imports
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

// Maps for the lists
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;


class ServerInstance {
    public String ip;
    public String port;
    public Process p;
    public String beginningRange;
    public String endRange;

    public ServerInstance (String ip, String port, Process p, String endRange, String beginningRange) {
        this.ip = ip;
        this.port = port;
        this.p = p;
        this.endRange = endRange;
        this.beginningRange = null;
    }
}

public class ECSClient {
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "ECSClient> ";
	private BufferedReader stdin;
	private boolean stop = false;
	KVStore kvClient = null;

    private int nodes = 0;
    private Process zp;
    private ZooKeeper zoo;
    private String metadataZNodePath;
    //final CountDownLatch connectedSignal = new CountDownLatch(1);
    private TreeMap<String, ServerInstance> idleServers;
    private TreeMap<String, ServerInstance> runningServers;

    private ZooKeeper zk = null; 
    final CountDownLatch connectionLatch = new CountDownLatch(1);

    public ECSClient(String configFile)
    {
        idleServers = new TreeMap<String, ServerInstance>();
        runningServers = new TreeMap<String, ServerInstance>();

        zp = null;
        try {
            parseConfig(configFile);
            zp = Runtime.getRuntime().exec("./zookeeper/zookeeper-3.4.9/bin/zkServer.sh start");
            zp.waitFor();
            this.zk = new ZooKeeper ("localhost", 2181, new Watcher() {
                public void process(WatchedEvent we) {
		            if (we.getState() == KeeperState.SyncConnected) {
			            connectionLatch.countDown();
		            }
	            }
            });

		    connectionLatch.await();
            
            this.zk.create("/DataMigrator", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);

            this.metadataZNodePath = "/Metadata";
            this.zk.create(this.metadataZNodePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void killRemoteServers(String ip) throws IOException {

        Process p = Runtime.getRuntime().exec("ssh -n " + ip + " ps -aux | grep ms3-server");
        String line = null;
        BufferedReader in = new BufferedReader(new InputStreamReader(
                                                    p.getInputStream(), "UTF-8"));

        while ((line = in.readLine()) != null) {
            System.out.println(line);
            String [] javaProcess = line.split(" ");
            Process k = Runtime.getRuntime().exec("ssh -n " + ip + " kill -9 " + javaProcess[1]);
 
        }
    }

    public String convert2MD5(String input)
    {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] messageDigest = md.digest(input.getBytes());
            BigInteger number = new BigInteger(1, messageDigest);
            String hashtext = number.toString(16);
            // Now we need to zero pad it if you actually want the full 32 chars.
            while (hashtext.length() < 32) {
                hashtext = "0" + hashtext;
            }
            return hashtext;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void printMap(TreeMap<String, ServerInstance> hmap) {
        for (Map.Entry<String, ServerInstance> entry : hmap.entrySet()) {
            System.out.println(entry.getKey()+" : "+entry.getValue().ip + " " + entry.getValue().port + " " + entry.getValue().endRange + " " + entry.getValue().beginningRange);
        }
    }

    public void parseConfig(String configFile) 
    {
        File cf = new File(configFile);
        boolean exists = cf.exists();
        if (!exists) 
        {
            System.out.println("Config file doesn't exist");
            System.exit(-1);
        }        

        try {
            BufferedReader br = null;
	        br = new BufferedReader(new FileReader(configFile));
            String line;
            while ((line = br.readLine()) != null)
            {
                String[] tokens = line.split("\\s+");
                // Should raise an exception if port is not an integer
                int portCheck = Integer.parseInt(tokens[2]);
                String endRange = convert2MD5(tokens[1] + tokens[2]);
                ServerInstance serv = new ServerInstance(tokens[1], tokens[2], null, endRange, null);
                idleServers.put(endRange, serv);            
            }
            printMap(idleServers);
        
            //sortByRange(idleServers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


	public void run() {
		while(!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
            
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
		        System.out.println("Running Shutdown Hook");
		        try {
                
                    for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet()) 
                    {
                        killRemoteServers(entry.getValue().ip);
                        if( getZNodeStats("/"+ entry.getValue().ip + entry.getValue().port + "/UpdateMetadata") != null)                   
                            zk.delete("/"+ entry.getValue().ip + entry.getValue().port + "/UpdateMetadata", -1);
                        if( getZNodeStats("/"+ entry.getValue().ip + entry.getValue().port) != null)                   
                            zk.delete("/"+ entry.getValue().ip + entry.getValue().port, -1);
                    }
                

                    for (Map.Entry<String, ServerInstance> entry : idleServers.entrySet()) 
                    {
                        if( getZNodeStats("/"+ entry.getValue().ip + entry.getValue().port + "/UpdateMetadata") != null)                   
                            zk.delete("/"+ entry.getValue().ip + entry.getValue().port + "/UpdateMetadata", -1);
                        if( getZNodeStats("/"+ entry.getValue().ip + entry.getValue().port) != null)                   
                            zk.delete("/"+ entry.getValue().ip + entry.getValue().port, -1);
                    }

                    if ( getZNodeStats("/DataMigrator") != null)
                        zk.delete("/DataMigrator", -1);   
                    if ( getZNodeStats("/Metadata") != null)
                        zk.delete("/Metadata", -1);

                    Process p = Runtime.getRuntime().exec("./zookeeper/zookeeper-3.4.9/bin/zkServer.sh stop");
                    //execute
                } catch (Exception e) {
                    e.printStackTrace();
                }
          }
        });

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

    private void updateMetaData(String key)
    {
        //NavigableMap<String, ServerInstance> myMap = runningServers;
        Map.Entry<String, ServerInstance> next = runningServers.higherEntry(key); // next
        Map.Entry<String, ServerInstance> prev = runningServers.lowerEntry(key);  // previous

        String prevEndRange;

        // If only element in list
        if (next == null && prev == null)
        {
            BigInteger br1 = new BigInteger(key, 16);
            br1.add(BigInteger.valueOf(1));
            ServerInstance val = runningServers.get(key);
            val.beginningRange = br1.toString(16);
            while (val.beginningRange.length() < 32) {
                val.beginningRange = "0" + val.beginningRange;
            }
            runningServers.put(key, val);
            return;
        }

        // First element
        if( prev == null) 
        {
            prevEndRange = runningServers.lastEntry().getValue().endRange;
        }               
        else 
        {             
            prevEndRange = prev.getValue().endRange;
        }


        BigInteger br = new BigInteger(prevEndRange, 16);
        //br.add(BigInteger.valueOf(1));
        ServerInstance val = runningServers.get(key);
        val.beginningRange = br.toString(16);
        while (val.beginningRange.length() < 32) {
            val.beginningRange = "0" + val.beginningRange;
        }
        runningServers.put(key, val);

 
        String endRange = val.endRange;
        BigInteger br_next = new BigInteger(endRange, 16);
        //br_next.add(BigInteger.valueOf(1));
        
        // Last element       
        if (next == null)
        {
            next = runningServers.firstEntry();
        }

        ServerInstance val_next = next.getValue();
        val_next.beginningRange = br_next.toString(16);
        while (val_next.beginningRange.length() < 32) {
            val_next.beginningRange = "0" + val_next.beginningRange;
        }

        runningServers.put(next.getKey(), val_next);        
    }


    private String getMetadata()
    {
        String metadata = "";
        for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet()) {
            metadata += entry.getValue().ip + "," + entry.getValue().port + "," + entry.getValue().beginningRange + "," + entry.getValue().endRange + ";";  
        }
        return metadata;
    }

	private void handleCommand(String cmdLine) {
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {
			stop = true;
            System.out.println(PROMPT + "Application exit!");

            try {
            
                for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet()) 
                {
                    killRemoteServers(entry.getValue().ip);
                    this.zk.delete("/"+ entry.getValue().ip + entry.getValue().port + "/UpdateMetadata", -1);
                    this.zk.delete("/"+ entry.getValue().ip + entry.getValue().port, -1);
                }
                this.zk.delete("/DataMigrator", -1);   
                this.zk.delete("/Metadata", -1);

                Process p = Runtime.getRuntime().exec("./zookeeper/zookeeper-3.4.9/bin/zkServer.sh stop");
                //execute
            } catch (Exception e) {
                e.printStackTrace();
            }


        // ssh-copy-id user@host
        // ssh-keygen -R <host>

		} else if (tokens[0].equals("initService")) {
			if(tokens.length == 4) {
				try{
                    nodes = Integer.parseInt(tokens[1]);
					String cacheSize = tokens[2];
                    String cacheStrategy = tokens[3];
                    int numberOfIdleKeys = idleServers.size();
                    if (numberOfIdleKeys < nodes)
                    {
                        System.out.println("ERROR: Nodes requested more than number of idle servers available");
                        return;
                    }

                    for (int i = 0; i < nodes; i++) 
                    {
                        Random       random    = new Random();
                        List<String> keys      = new ArrayList<String>(idleServers.keySet());
                        String       randomKey = keys.get( random.nextInt(keys.size()) );
                        ServerInstance       values     = idleServers.get(randomKey);
                        
                        ServerInstance serv = new ServerInstance(values.ip, values.port, null, randomKey, null);
                        idleServers.remove(randomKey);
                        runningServers.put(randomKey, serv);
                        updateMetaData(randomKey);

                        this.zk.create("/" + values.ip + values.port, "SERVER_STOPPED".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            
                        this.zk.create("/" + values.ip + values.port + "/UpdateMetadata", "False".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL);
                    } 

                    String metadata = getMetadata();
                    this.metadataZNodePath = "/Metadata";
                    updateZNode(this.metadataZNodePath, metadata.getBytes());

                    for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet()) {
                        String script = "ssh -n " + entry.getValue().ip + " nohup java -jar /nfs/ug/homes-1/b/bettadpu/419/2/oldcode/ms3-server.jar " +
                                 entry.getValue().port + " " + cacheSize + " " + cacheStrategy + " " + entry.getValue().ip + " &";

                        Runtime run = Runtime.getRuntime();
                        try {
                          Process p = run.exec(script);
                          System.out.println(script);
                          ServerInstance serv = entry.getValue();
                          serv.p = p;
                          runningServers.put(entry.getKey(), serv);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    printMap(runningServers);
                } catch (Exception e) {
                     e.printStackTrace();
                }
			} else {
				printError("Invalid number of parameters!");
			}

		} else  if (tokens[0].equals("start")) {
			if(tokens.length == 1) {
                for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet())
                {
                    String path = "/" + entry.getValue().ip + entry.getValue().port;
                    try {                    
                        updateZNode(path, "SERVER_STARTED".getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }                    
                }

			} else {
				printError("Invalid Arguments");
			}

		}
		else if(tokens[0].equals("stop")) {
			if(tokens.length == 1) {
                for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet())
                {
                    String path = "/" + entry.getValue().ip + entry.getValue().port;
                    try {                    
                        updateZNode(path, "SERVER_STOPPED".getBytes());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }                    
                }

			} else {
				printError("Invalid Arguments");
			}
		}
		else if(tokens[0].equals("addNode"))
		{

		      if(tokens.length == 3) {
  					String cacheSize = tokens[1];
                    String cacheStrategy = tokens[2];
                    if (idleServers.size() == 0) {
                        System.out.println("Error: No idle servers available!");
                        return;
                    }

                    Random       random    = new Random();
                    List<String> keys      = new ArrayList<String>(idleServers.keySet());
                    String       randomKey = keys.get( random.nextInt(keys.size()) );
                    ServerInstance       values     = idleServers.get(randomKey);
                    
                    ServerInstance serv = new ServerInstance(values.ip, values.port, null, randomKey, null);
                    idleServers.remove(randomKey);
                    runningServers.put(randomKey, serv);
                    updateMetaData(randomKey);

                    try {
                        
                        String metadata = getMetadata();
                        updateZNode(this.metadataZNodePath, metadata.getBytes());


                        this.zk.create("/" + values.ip + values.port, "SERVER_WRITE_LOCK_CONSUMER".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT);
                        this.zk.create("/" + values.ip + values.port + "/UpdateMetadata", "false".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT);

                        // Set the successor server to SERVER_WRITE_LOCK
                        Map.Entry<String, ServerInstance> next = runningServers.higherEntry(randomKey); // next
                        if (next == null)
                            next = runningServers.firstEntry();

                        updateZNode("/" + next.getValue().ip + next.getValue().port, "SERVER_WRITE_LOCK_PRODUCER".getBytes());

                        // Run the server
                        String script = "ssh -n " + values.ip + " nohup java -jar /nfs/ug/homes-1/b/bettadpu/419/2/oldcode/ms3-server.jar " +
                                 values.port + " " + cacheSize + " " + cacheStrategy + " " + values.ip + " &";

                        Runtime run = Runtime.getRuntime();
                        Process p = run.exec(script);
                        System.out.println(script);
                        serv.p = p;
                        runningServers.put(randomKey, serv);

                        while(!getZNodeData("/DataMigrator", false).equals("DONE"));

                        logger.info("Data transfer DONE");
                        print("Data transfer DONE");

                        for (Map.Entry<String, ServerInstance> entry : runningServers.entrySet()) {
                            updateZNode("/" + entry.getValue().ip + entry.getValue().port + "/UpdateMetadata", "True".getBytes());                            
                        }
                        
                        // Start the new server
                        updateZNode("/" + values.ip + values.port, "SERVER_STARTED".getBytes());
     
                        // Get the old Status of the successor and store it later
                        String oldStatusOfSuccessor = getZNodeData("/" + next.getValue().ip + next.getValue().port, false);
     
                        // Wait until successor update flag is false
                        while(!getZNodeData("/" + next.getValue().ip + next.getValue().port + "/UpdateMetadata", false).equals("False"));
                        logger.info("Successor's Metadata updated");                    
                        print("Successor's Metadata updated");                    
                        
                        // Tell the successor to delete it's old files
                        updateZNode("/" + next.getValue().ip + next.getValue().port, "SERVER_DELETE_FILES".getBytes());
                        logger.info("Successor's status changed to SERVER_DELETE_FILES");
                        print("Successor's status changed to SERVER_DELETE_FILES");

                        // Wait until successor deletes all it's files
                        while(!getZNodeData("/DataMigrator", false).equals("DELETE_COMPLETE"));
                        logger.info("Data DELETE_COMPLETE");
                        print("Data DELETE_COMPLETE");

                        // Resets the status of the successor
                        updateZNode("/" + next.getValue().ip + next.getValue().port, oldStatusOfSuccessor.getBytes());

                        // Reset datamigrator flag
                        updateZNode("/DataMigrator", "".getBytes());

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
              }
		}

		else if(tokens[0].equals("disconnect")) {
            if (kvClient != null)
    			kvClient.disconnect();
            else
                printError("Please connect to a server!");
            kvClient = null;

		} 
        
        else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT +
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command");
			printHelp();
		}
	}

	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("ECHO CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t Inserts a key and value into the server \n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t\t Retrieves the value associated with the key from the server \n");
		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");

		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT
				+ "Possible log levels are:");
		System.out.println(PROMPT
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

    private void print(String msg)
    {
        System.out.println(msg);
    }

	private String setLevel(String levelString) {

		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}


    
	public Stat getZNodeStats(String path) throws KeeperException,
			InterruptedException {
		Stat stat = zk.exists(path, true);
		if (stat != null) {
			; // System.out.println("Node exists and the node version is " + path
			//		+ stat.getVersion());
		} else {
			System.out.println("Node does not exists");
		}
		return stat;
	}


    public String getZNodeData(String path, boolean watchFlag) throws KeeperException,
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

	public void updateZNode(String path, byte[] data) throws KeeperException,
			InterruptedException {
		
        if(zk.exists(path, true) != null) {
            int version = this.zk.exists(path, true).getVersion();
		    this.zk.setData(path, data, version);
        }
        else {
            System.out.println("Node does not exist");
        }
	}


    /**
     * Main entry point for the echo server application.
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/ecs.log", Level.OFF);
            if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: <config file>!");
            }
            else {


                String configFile = args[0];
    			ECSClient app = new ECSClient(configFile);
    			app.run();
            }
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }
}
