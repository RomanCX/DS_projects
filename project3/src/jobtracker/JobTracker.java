package jobtracker;

import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import namenode.DatanodeInfo;
import mapredCommon.Job;
import mapredCommon.MapTask;
import protocals.JobTrackerProtocol;
import protocals.NamenodeProtocal;
import protocals.TkRegistration;

public class JobTracker implements JobTrackerProtocol {
	// port number of jobTracker
	private int jobTrackerPort;
	// max number of map tasks that can run in a tasktracker simultaneously
	private int maxMapTasks;
	// max number of reducer tasks that can run in a tasktracker simultaneously
	private int maxReduceTasks;
	// interval for heart beat
	private int heartBeatInterval;
	// protocol of namenode
	private NamenodeProtocal namenode;
	
	// some constants
	private static final String JOBTRACKER_RMI_NAME = "jobtracker";
	private static final String MAPRED_CONFIGURE_FILE="mapred.cnf";
	private static final String DFS_CONFIGURE_FILE="dfs.cnf";
	
	// tasktrackers
	HashMap<Integer, TaskTrackerInfo> taskTrackers;
	// running jobs
	HashMap<Integer, Job> runningJobs;
	
	public JobTracker() {
		taskTrackers = new HashMap<Integer, TaskTrackerInfo>();
		runningJobs = new HashMap<Integer, Job>();
	}
	
	public boolean init() {
		try {
			// get namenode
			Properties pro = new Properties();
			pro.load(new FileReader(DFS_CONFIGURE_FILE));
			String name = pro.getProperty("fs.default.name");
			int pos1 = name.indexOf("//");
			int pos2 = name.indexOf(":", pos1 + 2);
			String namenodeAddr = name.substring(pos1 + 2, pos2);
			int namenodePort = Integer.parseInt(name.substring(pos2 + 1));
			Registry registry = LocateRegistry.getRegistry(namenodeAddr, namenodePort);
			namenode = (NamenodeProtocal)registry.lookup("namenode");
			
			// load configuration for jobtracker
			pro.clear();
			pro.load(new FileReader(MAPRED_CONFIGURE_FILE));
			jobTrackerPort = Integer.parseInt(pro.getProperty("jobtracker.port"));
			maxMapTasks = Integer.parseInt(pro.getProperty("tasktracker.map.tasks.maximum"));
			maxReduceTasks = Integer.parseInt(pro.getProperty("tasktracker.reduce.tasks.maximum"));
			heartBeatInterval = Integer.parseInt(pro.getProperty("tasktracker.heartbeat.interval"));
			// To do load more parameters
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	@Override
	public TkRegistration register(String address, int port) {
		TaskTrackerInfo newTaskTracker = new TaskTrackerInfo(address, port);
		int numTaskTrackers = 0;
		synchronized(taskTrackers) {
			numTaskTrackers = taskTrackers.size();
			numTaskTrackers++;
			taskTrackers.put(numTaskTrackers, newTaskTracker);
		}
		return new TkRegistration(maxMapTasks + maxReduceTasks, numTaskTrackers,
								heartBeatInterval);
	}
	
	@Override
	public int submitJob(Job job) {
		synchronized(runningJobs) {
			numJobs++;
			job.setJobId(numJobs);
			runningJobs.put(numJobs, job);
		}
		TreeMap<Integer, List<DatanodeInfo>> blockToDn =
								namenode.getFileBlocks(job.getInputPath());
		int mapTaskId = 0;
		for (Map.Entry entry : blockToDn.entrySet()) {
			int blockId = (int)entry.getKey();
			List<DatanodeInfo> datanodes = (List<DatanodeInfo>)entry.getValue();
			MapTask map = new MapTask(mapTaskId, blockId, job);
			synchronized(jobToTask) {
				if (!jobToTask.contains(job.getjobId())) {
					List<Integer>
				}
			}
		}
	}
	
	@Override
	public HeartBeatResponse heartBeat(List<Integer> finishedTasks, int numSlots) {
		// To do, have no idea
	}
	
	public int getPort() {
		return jobTrackerPort;
	}
	
	public static void main(String[] args) throws Exception {
		JobTracker jobTracker = new JobTracker();
		if (jobTracker.init() == false) {
			System.exit(1);
		}
		JobTrackerProtocol jobTrackerStub = 
				(JobTrackerProtocol)UnicastRemoteObject.exportObject(jobTracker, 0);
		Registry registry = LocateRegistry.createRegistry(jobTracker.getPort());
		registry.rebind(JOBTRACKER_RMI_NAME, jobTrackerStub);
	}
}
