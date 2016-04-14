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
import java.io.UnsupportedEncodingException;
import java.util.List;

public class JobTracker {
    
	private final int DICT_SIZE = 265778;
    String myPath = "/jt-primary";
	String backup = "/jt-backup";
	String jobsPath = "/jobs";
	static int amPrimary = 0;
    ZkConnector zkc;
	ZooKeeper zk;
    Watcher watcher;

    public static void main(String[] args) {
      
        if (args.length != 1) {
            if(Debug.debug)System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. JobTracker zkServer:clientPort");
            return;
        }

        JobTracker j = new JobTracker(args[0]);   
        
		try {Thread.sleep(5000); } catch (Exception e) {}  
        j.checkpath();      

        while (true) {
			try { Thread.sleep(1000); } catch (Exception e) { } 
			j.checkJobs();
        }
    }

    public JobTracker(String hosts) {
        zkc = new ZkConnector();
        try {
            zkc.connect(hosts);
        } catch(Exception e) {
            if(Debug.debug)System.out.println("Zookeeper connect "+ e.getMessage());
        }
 
        watcher = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };

		zk = zkc.getZooKeeper();
		Stat stat = zkc.exists("/jobs", null);
		if (stat == null)
			zkc.create("/jobs", null, CreateMode.PERSISTENT);
	}
    
    private void checkpath() {

        Stat stat = zkc.exists(myPath, watcher);
        if (stat == null) {              // znode doesn't exist; let's try creating it
            if(Debug.debug)System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        "empty",           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
            if (ret == Code.OK) {
				if(Debug.debug)System.out.println("the primary!");
				amPrimary = 1;
			}
        } 
		if (amPrimary == 0) {
			//Primary exists, make me the backup
		
			if(Debug.debug)System.out.println("Creating " + backup);
			Code ret = zkc.create(
						backup,
						null,
						CreateMode.EPHEMERAL
						);
			if (ret == Code.OK) if(Debug.debug)System.out.println("the backup!");
		}
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                if(Debug.debug)System.out.println(myPath + " deleted! Let's go!");
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                if(Debug.debug)System.out.println(myPath + " created!");    
            	try{ Thread.sleep(1000); } catch (Exception e) {}
				checkpath(); // re-enable the watch
            }
        }
    }

	private void checkJobs() {
		if (amPrimary == 0) return;
		try {
			//jobs/<hash>
			List<String> children = zk.getChildren(jobsPath, null);
			if (children.size() == 0) {
				try { Thread.sleep(1000); } catch (Exception e) { }
				return;
			}
			for (int j = 0; j < children.size(); j++) {
				Stat stat = zkc.exists("/task", null);
				if (stat == null) {
					Code ret = zkc.create("/task", null, CreateMode.PERSISTENT);
					if (ret == Code.OK) if(Debug.debug)System.out.println("Created /task root");
					else if(Debug.debug)System.out.println("Didn't create /task root");
				}
				if(Debug.debug)System.out.println("Created job: " + children.get(j));
				//Create a task for each partition. label as <job hash>-<partition #>
				for (int i = 0; i <= Math.ceil(DICT_SIZE/1000); i++) {
					String thePath = "/task/" + children.get(j) + "-" + i;
					stat = zkc.exists(thePath, null);
					if (stat == null)
						zkc.create(thePath, children.get(j), CreateMode.PERSISTENT);	// We want to these manually
				}
				stat = zkc.exists("/jobs/"+children.get(j), null);
				if (stat != null)
					zk.delete("/jobs/"+children.get(j), stat.getVersion());
			}
			
			checkpath(); //reset watch
		} catch (KeeperException e) { e.printStackTrace();
		} catch (InterruptedException e) { e.printStackTrace(); }
	}

}
