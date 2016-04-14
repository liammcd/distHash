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
import java.util.List;

public class FileServer {
    
	private final int DICT_SIZE = 265748;
	private int lines = 0;	// Will be updated to # of lines in dictionary
	String dict[];	// The dictionary
    String myPath = "/fs-primary";
	String backup = "/fs-backup";
	String taskPath = "/task";
	int amPrimary = 0;
    ZkConnector zkc;
    Watcher watcher;
	Watcher fsWatch;

	List<String> childList;
	
    public static void main(String[] args) {
      
        if (args.length != 2) {
            if(Debug.debug)System.out.println("Usage: java -classpath lib/zookeeper-3.3.2.jar:lib/log4j-1.2.15.jar:. FileServer zkServer:clientPort");
            return;
        }

        FileServer f = new FileServer(args[0], args[1]);   
 
        if(Debug.debug)System.out.println("Sleeping...");
        try {
            Thread.sleep(5000);
        } catch (Exception e) {}
        
        f.checkpath();
        
        if(Debug.debug)System.out.println("Sleeping...");
        while (true) {
            try{ Thread.sleep(5000); } catch (Exception e) {}
        }
    }

    public FileServer(String hosts, String file) {
        zkc = new ZkConnector();
		dict = new String[DICT_SIZE];
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

 		fsWatch = new Watcher() { // Anonymous Watcher
                            @Override
                            public void process(WatchedEvent event) {
                                handleEvent(event);
                        
                            } };
							
		//Load from file
		try {
			loadFile(file);
		} catch(IOException e) {
			//Oh shit!
		}
    }
    
    private void checkpath() {
        Stat stat = zkc.exists(myPath, watcher);
		Stat stat2;
        if (stat == null) {              // znode doesn't exist; let's try creating it
            if(Debug.debug)System.out.println("Creating " + myPath);
            Code ret = zkc.create(
                        myPath,         // Path of znode
                        null,           // Data not needed.
                        CreateMode.EPHEMERAL   // Znode type, set to EPHEMERAL.
                        );
			amPrimary = 1;	//I am the primary
            if (ret == Code.OK) if(Debug.debug)System.out.println("the primary!");

			//Create persistent /fs (file serve) node
		//	ret = zkc.create("/fs", null, CreateMode.PERSISTENT);
		//	if (ret == Code.OK) if(Debug.debug)System.out.println("Created /fs node");
		//	populateNode();
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
		else {
			stat = zkc.exists("/fs", null);
			if (stat == null) {
				zkc.create("/fs", null, CreateMode.PERSISTENT);
			}
			populateNode();
		}	
    }

    private void handleEvent(WatchedEvent event) {
        String path = event.getPath();
        EventType type = event.getType();
		if(Debug.debug)System.out.println("Handling event " + type);
        if(path.equalsIgnoreCase(myPath)) {
            if (type == EventType.NodeDeleted) {
                if(Debug.debug)System.out.println(myPath + " deleted! Let's go!");       
                checkpath(); // try to become the boss
            }
            if (type == EventType.NodeCreated) {
                if(Debug.debug)System.out.println(myPath + " created!");       
                try{ Thread.sleep(5000); } catch (Exception e) {}
                checkpath(); // re-enable the watch
            }
        }
    }

	private void loadFile(String file) throws IOException {
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		String line;
		while ((line = br.readLine()) != null) {
			dict[lines] = line;
			lines++;		
		}
		if(Debug.debug)System.out.println("Total lines: " + Integer.toString(lines));
	}

	private void printDict() {
		for (int i = 0; i < lines; i++) {
			if(Debug.debug)System.out.println(dict[i]);
		}
	}

	/* Create fs sub nodes for each partition*/
	private void populateNode() {
		for (int i = 0; i <= Math.ceil(DICT_SIZE/1000); i++) {
			String data = new String();
			int partition = i;	//Node format is <hash>-<partition #>

			Stat stat = zkc.exists("/fs/" + partition, null);
			if (stat != null)
				continue;
			for (int j = 0; j < 1000; j++) {
				if ( (partition * 1000) + j > lines-1) break;
				data += dict[(partition*1000)+j];
				data += " ";
			}
			Code ret = zkc.create("/fs/" + partition, data, CreateMode.PERSISTENT);
			if (ret == Code.OK)
				if(Debug.debug)System.out.println("Created partition " + partition);
		}
	}

}
