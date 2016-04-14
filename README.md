# distHash
Distributed dictionary hash attacker in Java

Uses Apache Zookeeper to implement a distributed system that performs dictionary attack hash lookups and returns the result.

Build by using the Makefile in the src/ directory.

Use the scripts in the scripts/ folder to start the various components: File server, job tracker, workers, and the client driver.
The system will handle multiple jobs and implements fault tolerance for any killed services.

The <ip> and <port> entries below should be the address of a ZooKeeper server. See the [ZooKeeper website](https://zookeeper.apache.org/releases.html) to get a release

Worker:

	./start_worker.sh <ip> <port>

FileServer:

	./start_fileserver.sh <ip> <port>

JobTracker:

	./start_jobtracker.sh <ip> <port>

To submit a password to be cracked:

	./submit_job.sh <ip> <port> <hash>

To check the status of the job:

	./check_job_status.sh <ip> <port> <hash>
