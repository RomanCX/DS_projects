package jobtracker;

import java.io.FileReader;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import namenode.DatanodeInfo;
import mapredCommon.Job;
import mapredCommon.MapTask;
import mapredCommon.Task;
import protocals.HeartBeatResponse;
import protocals.JobTrackerProtocol;
import protocals.NamenodeProtocal;
import protocals.TkRegistration;

public class JobTracker implements JobTrackerProtocol {
	
	// some constants
	private static final String JOBTRACKER_RMI_NAME = "jobtracker";
	private static final String MAPRED_CONFIGURE_FILE="mapred.cnf";
	private static final String DFS_CONFIGURE_FILE="dfs.cnf";
		
	// port number of jobTracker
	private int jobTrackerPort;
	// max number of map tasks that can run in a tasktracker simultaneously
	private int maxMapTasks;
	// max number of reducer tasks that can run in a tasktracker simultaneously
	private int maxReduceTasks;
	// interval for heart beat
	private int heartBeatInterval;
	// directory for storing inputs and outputs of tasks
	private String tmpDir;
	// protocol of namenode
	private NamenodeProtocal namenode;
	
	// next available job id
	private int nextJobId;
	// next available task id
	private int nextTaskId;
	// number of map tasks to be done
	private int mapTasksLeft;
	// number of reduce tasks to be done
	private int reduceTasksLeft;
	// the job being executed
	Job currentJob;
	// a queue storing jobs to be executed
	Queue<Job> jobQueue;
	// trakcerId -> taskTracker
	HashMap<Integer, TaskTrackerInfo> trackerIdToTracker;
	// taskId -> task
	HashMap<Integer, Task> taskIdToTask;
	// list of taskId
	List<Integer> jobTasks;
	// address -> list of taskId
	TreeMap<String, List<Integer>> dnToTaskIds;
	// tasktracker -> list of ids of running tasks
	HashMap<Integer, Set<Integer>> trackerIdToRunningTaskIds;
	// tasktracker -> list of ids of completed tasks
	HashMap<Integer, Set<Integer>> trackerIdToCompletedTasksIds;
	
	public JobTracker() {
		
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
	
	public int getPort() {
		return jobTrackerPort;
	}
	
	/* Register a tasktracer */
	@Override
	public TkRegistration register(String address, int port) {
		TaskTrackerInfo newTaskTracker = new TaskTrackerInfo(address, port);
		int numTaskTrackers = 0;
		synchronized(trackerIdToTracker) {
			numTaskTrackers = trackerIdToTracker.size();
			numTaskTrackers++;
			trackerIdToTracker.put(numTaskTrackers, newTaskTracker);
		}
		return new TkRegistration(maxMapTasks + maxReduceTasks, numTaskTrackers,
								heartBeatInterval, tmpDir);
	}
	
	/* Make a job retired */
	private void retireJob() {
		/* To do clear some data structures */
		
		if (jobQueue.isEmpty() == false) {
			currentJob = jobQueue.poll();
			prepareJob();
		}
	}
	
	/* Generate tasks for current job */
	private void prepareJob() {
		// get the number of blocks in input file
		TreeMap<Integer, List<DatanodeInfo>> blockToDn =
							namenode.getFileBLocks(currentJob.getInputPath());
		for (Map.Entry entry : blockToDn.entrySet()) {
			int blockId = (int)entry.getKey();
			List<DatanodeInfo> datanodes = (List<DatanodeInfo>)entry.getValue();
			// for every block, generate a map task
			MapTask map = new MapTask(nextTaskId, blockId, currentJob);
			taskIdToTask.put(nextTaskId, map);
			jobTasks.add(nextTaskId);
			// add this map task to each node which has input file in local
			for (DatanodeInfo datanode : datanodes) {
				String address = datanode.getAddress();
				if (dnToTaskIds.containsKey(address) == false)
					dnToTaskIds.put(address, new ArrayList<Integer>());
				List<Integer> taskIds = dnToTaskIds.get(address);
				taskIds.add(nextTaskId);
			}
		}
	}
	
	private void generateReduceTasks() {
		/* if user specified number of reduce tasks, then use this number
		 * else calculate a number according the to number of map tasks
		 * for each reduce task, generate a taskId, pass the job and list 
		 * of tasktrackerinfo to it
		 */
		int reduceTasksLeft = currentJob.getNumReduceTasks();
		if (reduceTasksLeft == 0) {
			reduceTasksLeft = maxReduceTasks * trackerIdToTracker.size();
		}
		
	}
	
	private Task selectTask(int taskTrackerId) {
		String address = trackerIdToTracker.get(taskTrackerId).getAddress();
		List<Integer> taskIds = dnToTaskIds.get(address);
		if (taskIds != null) {
			return taskIdToTask.get(taskIds.remove(0));
		}
		else {
			String nextAddress = dnToTaskIds.higherKey(address);
			if (nextAddress == null)
				nextAddress = dnToTaskIds.lowerKey(address);
			taskIds = dnToTaskIds.get(nextAddress);
			return taskIdToTask.get(taskIds.remove(0));
		}
	}
	
	/* Accept job submitted by client */
	@Override
	public synchronized int submitJob(Job job) {
		job.setJobId(nextJobId);
		jobQueue.add(job);
		nextJobId++;
			
		return job.getJobId();
	}
	
	@Override
	public HeartBeatResponse heartBeat(List<Integer> finishedTasks, int numSlots,
								int taskTrackerId) {
		/* based on finished tasks, update some data structures
		 * if all the map tasks are done, then generate reducer tasks
		 * if all the reducer tasks are done, then make the current retired
		 * and get new job
		 */
		Set<Integer> runningTaskIds = trackerIdToRunningTaskIds.get(taskTrackerId);
		Set<Integer> completedTaskIds = trackerIdToCompletedTasksIds.get(taskTrackerId);
		for (int taskId : finishedTasks) {
			runningTaskIds.remove(taskId);
			if (taskIdToTask.get(taskId) instanceof MapTask) {
				mapTasksLeft--;
				completedTaskIds.add(taskId);
				if (mapTasksLeft == 0)
					generateReduceTasks();
			}
			else {
				reduceTasksLeft--;
				if (reduceTasksLeft == 0) {
					retireJob();
				}
			}
		}
		
		/* if numSlots == 0, do nothing
		 * if numSlots > 0
		 *     if currentJob != null assign tasks
		 *     else do nothing
		 */
		if (numSlots == 0 || currentJob == null)
			return new HeartBeatResponse(new ArrayList<Task>());
		
		List<Task> tasksToBeAssigned = new ArrayList<Task>();
		while (numSlots > 0) {
			Task task = selectTask(taskTrackerId);
			if (task == null)
				break;
			tasksToBeAssigned.add(task);
		}
		return new HeartBeatResponse(tasksToBeAssigned);
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
