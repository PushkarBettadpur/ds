package app_kvServer;

import java.io.FileReader;
import java.io.BufferedReader;

import java.io.File;
import java.util.List;

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


import org.apache.log4j.*;


public class DataMigrator implements Runnable {
    private static Logger logger = Logger.getRootLogger();

	public void run() {
  
         while(true){

             try {

                Thread.sleep(500);

                if(KVServer.getZNodeData(KVServer.zNodePath+"/UpdateMetadata", false).equals("True"))
                {   
                    logger.info("BEGIN Updating Metadata");
                    String meta = KVServer.getZNodeData("/Metadata", false);
                    String[] tokens = meta.split(";");
                    for (int i = 0; i < tokens.length; i++)
                    {
                        String servStr = tokens[i];
                        String[] tok = servStr.split(",");
                        if (tok[0].equals(KVServer.ip) && tok[1].equals(Integer.toString(KVServer.port)))
                        {
                            KVServer.putBeginRange(tok[2]);
                            KVServer.putEndRange(tok[3]);
                        }
                    } 

                    KVServer.updateZNode(KVServer.zNodePath + "/UpdateMetadata", "False".getBytes());
                    logger.info("END Updating Metadata"); 
                }

                if(KVServer.getZNodeData(KVServer.zNodePath, false).equals("SERVER_DELETE_FILES"))
                {

                    File filesdir = new File("/nfs/ug/homes-1/b/bettadpu/419/2/oldcode/files"+KVServer.zNodePath);

                    logger.info("Deleting out of range keys");
                    /* If we're here, we're assuming Put requests will never go through bc the status should be write lock */
                    for (File f : filesdir.listFiles()) {
                        String fn = f.getName();
                        if (!fn.endsWith(".lock")  && !KVServer.store.isKeyInRange(fn, KVServer.getBeginRange(), KVServer.getEndRange())) {
                            try {
                                logger.info("Deleting File: " + fn);
                                f.delete();
                            } catch (Exception e) {
                                logger.info("Deletion File Exception: " + e);
                            }
                        }
                    }

                    KVServer.updateZNode("/DataMigrator", "DELETE_COMPLETE".getBytes());
                    
                }
 

                else if(KVServer.getZNodeData(KVServer.zNodePath, false).equals("SERVER_WRITE_LOCK_PRODUCER"))
                {
                    if (KVServer.getZNodeData("/DataMigrator", false).equals("WRITE_COMPLETE") ||
                        KVServer.getZNodeData("/DataMigrator", false).equals("DONE") )
                        continue;
                    
                    String newBeginRange = "";
                    String newEndRange = "";

                    String meta = KVServer.getZNodeData("/Metadata", false);
                    String[] tokens = meta.split(";");
                    for (int i = 0; i < tokens.length; i++)
                    {
                        String servStr = tokens[i];
                        String[] tok = servStr.split(",");
                        if (tok[0].equals(KVServer.ip) && tok[1].equals(Integer.toString(KVServer.port)))
                        {
                            newBeginRange = tok[2];
                            newEndRange = tok[3];
                        }
                    } 

                    KVServer.updateZNode("/DataMigrator", "WRITE_IN_PROGRESS".getBytes());
                    
                    File filesdir = new File("/nfs/ug/homes-1/b/bettadpu/419/2/oldcode/files"+KVServer.zNodePath);

                    /* If we're here, we're assuming Put requests will never go through bc the status should be write lock */
    
                    logger.info("Number of files in the Directory: " + Integer.toString(filesdir.listFiles().length));
                    for (File f : filesdir.listFiles()) {
                        String fn = f.getName();
                        if (!fn.endsWith(".lock") && !KVServer.store.isKeyInRange(fn, newBeginRange, newEndRange)) {
                            try {
                                logger.info("Transferring File: " + fn);
                                BufferedReader br = new BufferedReader(new FileReader("/nfs/ug/homes-1/b/bettadpu/419/2/oldcode/files"+KVServer.zNodePath+"/"+fn));
		                        String value = br.readLine();
                                if (br != null)
                				    br.close();
                                KVServer.zk.create("/DataMigrator/" + fn, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                                    CreateMode.EPHEMERAL);
                            } catch (Exception e) {
                                logger.info("File IO Exception: " + e);
                            }
                        }
                    }

                    KVServer.updateZNode("/DataMigrator", "WRITE_COMPLETE".getBytes());
                    
                }
  
                else if (KVServer.getZNodeData(KVServer.zNodePath, false).equals("SERVER_WRITE_LOCK_CONSUMER"))
                {
                    if (KVServer.getZNodeData("/DataMigrator", false).equals("DONE"))
                        continue;

                    List<String> list = KVServer.zk.getChildren("/DataMigrator", true);

                    if (list.size() == 0 && KVServer.getZNodeData("/DataMigrator", false).equals("WRITE_COMPLETE"))
                    {
                        KVServer.updateZNode("/DataMigrator", "DONE".getBytes());
                    }

                    for (int i = 0; i < list.size(); i++)
                    {
        				String value = KVServer.getZNodeData("/DataMigrator/"+list.get(i), false);
                        if (value != null)
                            KVServer.store.putHelper(list.get(i), value);
                        KVServer.zk.delete("/DataMigrator/"+list.get(i), -1);
		            }
                }
                

            } catch (Exception e) {
                e.printStackTrace();
            }

        }

    }

};
