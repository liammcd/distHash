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
import org.apache.zookeeper.Watcher.Event.EventType;

import java.io.IOException;
import java.io.FileReader;	
import java.io.BufferedReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Collections;

public class ClientDriver {

	private final int DICT_SIZE = 265778;
	String jobPath = "/job";
	String statusPath = "/status";
	String jtPath = "/jt-primary";
  	ZkConnector zkc;
    Watcher watcher;
	Watcher selfWatch;

	private long sessionId;
	static private int jobType = 0;
	static private int primaryOnline = 0;
	static private int replied = 0;
	private int submitted = 0;
	String job;

	public static void main(String[] args) {

		ClientDriver cd = new ClientDriver(args[0], args[1], args[2]);
		cd.checkpath();		
		while (primaryOnline == 0) {
			try {Thread.sleep(5000); } catch (Exception e) {}  // take a nap
		}
	}

	// Type is "job" or "status"
	ClientDriver(String hosts, String type, String theJob) {
		job = theJob;
		// Connect to zookeeper
		zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            if(Debug.debugClient)System.out.println("Zookeeper connect "+ e.getMessage());
        }
		
		if (type.equals("status")) {
			jobType = 0;		
		}
		else if (type.equals("job")) {
			jobType = 1;
		}	

		watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);     
							} };
		selfWatch = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);     
							} };
		
		sessionId = zkc.getZooKeeper().getSessionId();
		if(Debug.debugClient)System.out.println("Session id is " + Long.toString(sessionId));
		Code ret = zkc.create("/client-" + Long.toString(sessionId), null, CreateMode.EPHEMERAL);
		if (ret == Code.OK) if(Debug.debugClient)System.out.println("Created client node /client-" + Long.toString(sessionId));
		
		Stat stat = zkc.exists("/jobs", null);
		if (stat == null)
			zkc.create("/jobs", null, CreateMode.PERSISTENT);
	}

	private void checkpath() {
		Stat stat = zkc.exists(jtPath, watcher);
		Stat stat2 = zkc.exists("/client-" + Long.toString(sessionId), selfWatch);
		if (stat == null) {              // znode doesn't exist; let's try creating it
			if(Debug.debugClient)System.out.println("Job tracker is offline");
			try {Thread.sleep(5000); } catch (Exception e) {}  // take a nap
		} 
		else if (submitted == 0) {
			if(Debug.debugClient)System.out.println("Job tracker is online");
			submitTask(stat);
			primaryOnline = 1;
		}	
    }

    private void handleEvent(WatchedEvent event) {
		String path = event.getPath();
        EventType type = event.getType();
		if(path.equalsIgnoreCase(jtPath)) {
			if (primaryOnline == 1)
				return; // Don't reset the watches
		}
		String clientPath = "/client-"+Long.toString(sessionId);
        if(path.equalsIgnoreCase(clientPath)) {
			if (replied == 1)
				return;
			// Got an update
			// Get data from node
			try {
				if(Debug.debugClient)System.out.println("Got response for query");
				String reply = new String(zkc.getZooKeeper().getData(clientPath, null, null), "UTF-8");
				System.out.println(reply);
				replied = 1;
			} catch (UnsupportedEncodingException e) { e.printStackTrace(); 
			} catch (KeeperException e) { e.printStackTrace();
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
		checkpath();
    }

	private void submitTask(Stat stat) {
		submitted = 1;
		String jobString = new String();
		if (jobType == 0) {	// Status request
			jobString = Long.toString(sessionId) + "-status-" + job;
			if(Debug.debugClient)System.out.println("Checking status of job " + job);
			checkStatus();
			return;
		}
		else if (jobType == 1) { // Submit job
			jobString = Long.toString(sessionId) + "-job-" + job;
			if(Debug.debugClient)System.out.println("Submitting job " + job);
		}

		Code ret;			
		Stat stat2 = zkc.exists("/jobs/" + job, null);
		if (stat2 == null) {
			ret = zkc.create("/jobs/" + job, null, CreateMode.PERSISTENT);
			if(ret == Code.OK) if(Debug.debugClient)System.out.println("Pass");
			else if(Debug.debugClient)System.out.println("Fail");
		}
	}

	private void checkStatus() {
		Stat stat = zkc.exists("/results/" + job, null);
		if (stat == null) {
			System.out.println("Failed: Job not found");
		}
		else {
			try {
				List<String> children = zkc.getZooKeeper().getChildren("/results/" + job, null);
			
				if (children.size() < Math.ceil(DICT_SIZE/1000)+1) { //Don't have all the results yet //Pretend we got DICT_SIZE from the fileserver
					System.out.println("In Progress");
				}
				else {
					//Look at data of children, see if any have passed
					int found = 0;
					String password = new String();
					String returnString = new String();
					for (int i = 0; i < children.size(); i++) {
						String partition = children.get(i);
						String aResult = new String();
						try {
							aResult = new String(zkc.getZooKeeper().getData("/results/" + job + "/" + partition, null, null), "UTF-8");
						} catch (UnsupportedEncodingException e) { e.printStackTrace(); }

						String parts[] = aResult.split("-");
						if (parts[0].equalsIgnoreCase("PASS")) {
							// Got a match
							password = parts[1];
							found = 1;
							break;
						}
					}
					if (found == 1) {
						returnString = "Password found: " + password;
					}
					else
						returnString = "Failed: Password not found";
						
					System.out.println(returnString);
				}
			} catch (KeeperException e) { e.printStackTrace();
			} catch (InterruptedException e) { e.printStackTrace(); }
		}
	}

}
