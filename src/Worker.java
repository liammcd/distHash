/*******************************
 * Liam McDermott
 * 2016
 *******************************/

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Collections;

public class Worker {
    
    String myPath = "/workerpool";
	String fsPath = "/fs-primary";
	String taskPath = "/task";

    ZkConnector zkc;
	ZooKeeper zk;
    Watcher watcher;
	Watcher taskWatch;
	Watcher fsWatch;
	List<String> childList;
	String hash;
	
	protected static final List<ACL> acl = Ids.OPEN_ACL_UNSAFE;

	private int finishedTask = 0;
	private int resultExists = 0;
	private String id;
	private String myTask;

    public static void main(String[] args) {
      
        if (args.length != 1) {
            if(Debug.debug)System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. Worker zkServer:clientPort");
            return;
        }

       	Worker w = new Worker(args[0]);   
 
        if(Debug.debug)System.out.println("Sleeping...");
 		try { Thread.sleep(5000); } catch (Exception e) { }
        while (true) {
			w.checkpath();
        }
    }

    public Worker(String hosts) {
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            if(Debug.debug)System.out.println("Zookeeper connect "+ e.getMessage());
        }
		zk = zkc.getZooKeeper();
 
		Stat stat = zkc.exists("/workerpool", null);
		if (stat == null)
			zkc.create("/workerpool", null, CreateMode.PERSISTENT);
			
		Code ret = zkc.create(myPath + "/worker", null, CreateMode.EPHEMERAL_SEQUENTIAL);
		if (ret == Code.OK) if(Debug.debug)System.out.println("Created a worker child");

		stat = zkc.exists("/results", null);
		if (stat == null)	
			zkc.create("/results", null, CreateMode.PERSISTENT);

        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };

		taskWatch = new Watcher() { // Anonymous Watcher
                        	@Override
		                    public void process(WatchedEvent event) {
		                        handleEvent(event);
		                
		                    } };
		fsWatch = new Watcher() { // Anonymous Watcher
		                    @Override
		                    public void process(WatchedEvent event) {
		                        handleEvent(event);
		                
		                    } };

    }
    
    private void checkpath() {
        Stat stat = zkc.exists(fsPath, watcher);
		Stat stat2 = zkc.exists(taskPath, taskWatch);
		if (stat == null) {
			if(Debug.debug)System.out.println("Primary fs offline!");
		}
		else {
		//	if(Debug.debug)System.out.println("Primary fs online!");

			if (stat2 != null) {
				//if(Debug.debug)System.out.println("/task znode online");
				try{
				
					childList = zk.getChildren(taskPath, null);
					if (childList.size() == 0) {
						//if(Debug.debug)System.out.println("No children in /task znode");
						return;
					}
					
					Collections.shuffle(childList);
					myTask = childList.get(0);
					String parts[] = myTask.split("-");
					hash = parts[0];
					id = parts[1];					

					makeResultNodes();
					stat = zkc.exists("/results/" + hash + "/" + id, null);
					if (stat != null) return;
					
					stat = zkc.exists("/task/" + myTask, null);
					if (stat == null) return;

					// get lock on node
					String lockPath = "/task/" + myTask + "/lock-";
					String path = zk.create(lockPath, null, acl, CreateMode.EPHEMERAL_SEQUENTIAL);
					List<String> children = zk.getChildren("/task/" + myTask, null);
					
					String seqParts[] = path.split("-");
					int lowest = Integer.parseInt(seqParts[2]);
					int mySeq = lowest;
			
					for (int i = 0 ; i < children.size(); i++) {
						String moreParts[] = children.get(i).split("-");
						int seq = Integer.parseInt(moreParts[1]);
						if (seq < lowest)
							lowest = seq;
					}
					if (mySeq != lowest)
						return;

					stat = zkc.exists("/results/" + hash + "/" + id, null);
					if (stat != null) return;

					if(Debug.debug)System.out.println("Proceeding with task: " + myTask);
				
					if(Debug.debug)System.out.println("My task partition is: " + id);
			
					if(Debug.debug)System.out.println("My hash is: " + hash);
					
					processPartition();

					List<String> locks = zk.getChildren("/task/" + myTask, null);
					for (int i = 0; i < locks.size(); i++) {
						String deleteThis = "/task/" + myTask + "/" + locks.get(i);
						stat = zkc.exists(deleteThis, null);
						if (stat != null)
							zk.delete(deleteThis, stat.getVersion());
					}
					stat = zkc.exists("/task/" + myTask, null);
					if (stat != null)
						zk.delete("/task/" + myTask, stat.getVersion());
					
				} catch (KeeperException e) { 
				} catch (InterruptedException e) { e.printStackTrace(); }
			}
			else {
				if(Debug.debug)System.out.println("Task znode down");
			}	
		}
    }

    private void handleEvent(WatchedEvent event) {
	  	String path = event.getPath();
        EventType type = event.getType();
		if (path.equals(taskPath)) {
			//checkpath(); 
		}
    }
	
	private String crack(String dict) {
	
		MD5 hasher = new MD5();
		String[] splitDict = dict.split("\\s+");

		for (int i = 0; i < splitDict.length; i++) {
			String lineHash = hasher.getHash(splitDict[i]);
			if (hash.equals(lineHash)) {
				if(Debug.debug)System.out.println("Found matching hash!");
				return splitDict[i];
			}
		}

		return null;
	}
	
	private void makeResultNodes() {
	
		Stat stat = zkc.exists("/results/" + hash, null);
		if (stat == null)
			zkc.create("/results/" + hash, null, CreateMode.PERSISTENT);
	}

	private void processPartition() {
		try {
			Stat stat = zkc.exists("/fs/" +id, null);
			while ( stat == null) {
				stat = zkc.exists("/fs/" +id, null);
			}
			String dict = new String(zk.getData("/fs/" + id, null, null), "UTF-8");
		
			// Do the hash stuff
			String result = crack(dict);
		
			String resultPath = "/results/" + hash + "/" + id;
			String resultReturn = new String();
			Code ret;		

			if (result == null) {
				if(Debug.debug)System.out.println("Failure");
				//Wasn't found, report failure
			
				resultReturn = "FAIL";

				stat = zkc.exists(resultPath, null);
				if (stat == null)
					zkc.create(resultPath, resultReturn, CreateMode.PERSISTENT);
			}
			else {
				if(Debug.debug)System.out.println("Success! Password is: " + result);
				resultReturn = "PASS-" + result;	
			
				stat = zkc.exists(resultPath, null);
				if (stat == null) zkc.create(resultPath, resultReturn, CreateMode.PERSISTENT);	
			}
	
		} catch (KeeperException e) { e.printStackTrace();
		} catch (InterruptedException e) { e.printStackTrace();
		} catch (UnsupportedEncodingException e) { e.printStackTrace(); }	
	}

}
