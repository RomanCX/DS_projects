package jobtracker;

import java.io.FileReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;

import namenode.DatanodeInfo;
import mapredCommon.Job;
import mapredCommon.MapTask;
import mapredCommon.ReduceTask;
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
	// max number of map tasks that can run in a task tracker simultaneously
	private int maxMapTasks;
	// max number of reducer tasks that can run in a task tracker simultaneously
	private int maxReduceTasks;
	// interval for heart beat
	private int heartBeatInterval;
	// directory for storing inputs and outputs of tasks
	private String tmpDir;
	// protocol of name node
	private NamenodeProtocal namenode;
	
	// next available job id
	private int nextJobId;
	// next available taskTracker id
	private int nextTaskTrackerId;
	// next available map task id
	private int nextMapTaskId;
	// number of map tasks to be done
	private int mapTasksLeft;
	// number of reduce tasks to be done
	private int reduceTasksLeft;
	// the job being executed
	Job currentJob;
	// a queue storing jobs to be executed
	Queue<Job> jobQueue;
	// list of taskTrackers
	List<TaskTrackerInfo> taskTrackers;
	// trakcerId -> taskTracker
	HashMap<Integer, TaskTrackerInfo> trackerIdToTracker;
	// map tasks of current job
	List<MapTask> mapTasks;
	// reduce tasks of current job
	List<ReduceTask> reduceTasks;
	// reduce tasks to be ran
	List<Integer> reduceTasksToBeRan;
	// address -> list of taskId
	TreeMap<String, Set<Integer>> dnToMapTaskIds;
	// taskId -> list of address
	HashMap<Integer, List<String>> mapTaskIdToDns;
	// address of data node -> port of data node
	HashMap<String, Integer> addressToPort;
	// task tracker -> list of id of running tasks
	HashMap<Integer, Set<Integer>> trackerIdToRunningTaskIds;
	// task tracker -> list of id of completed tasks
	HashMap<Integer, Set<Integer>> trackerIdToCompletedMapTaskIds;
	
	public JobTracker() {
		nextJobId = 1;
		nextTaskTrackerId = 0;
		currentJob = null;
		jobQueue = new LinkedList<Job>();
		taskTrackers = new ArrayList<TaskTrackerInfo>();
		trackerIdToTracker = new HashMap<Integer, TaskTrackerInfo>();
		mapTasks = new ArrayList<MapTask>();
		reduceTasks = new ArrayList<ReduceTask>();
		reduceTasksToBeRan = new ArrayList<Integer>();
		dnToMapTaskIds = new TreeMap<String, Set<Integer>>();
		mapTaskIdToDns = new HashMap<Integer, List<String>>();
		addressToPort = new HashMap<String, Integer>();
		trackerIdToRunningTaskIds = new HashMap<Integer, Set<Integer>>();
		trackerIdToCompletedMapTaskIds = new HashMap<Integer, Set<Integer>>();
	}
	
	public void loadConfiguration() throws Exception {
		// get name node
		Properties pro = new Properties();
		pro.load(new FileReader(DFS_CONFIGURE_FILE));
		String name = pro.getProperty("fs.default.name");
		int pos1 = name.indexOf("//");
		int pos2 = name.indexOf(":", pos1 + 2);
		String namenodeAddr = name.substring(pos1 + 2, pos2);
		int namenodePort = Integer.parseInt(name.substring(pos2 + 1));
		Registry registry = LocateRegistry.getRegistry(namenodeAddr, namenodePort);
		namenode = (NamenodeProtocal)registry.lookup("namenode");
			
		// load configuration for job tracker
		pro.clear();
		pro.load(new FileReader(MAPRED_CONFIGURE_FILE));
		jobTrackerPort = Integer.parseInt(pro.getProperty("jobtracker.port"));
		maxMapTasks = Integer.parseInt(pro.getProperty("tasktracker.map.tasks.maximum"));
		maxReduceTasks = Integer.parseInt(pro.getProperty("tasktracker.reduce.tasks.maximum"));
		heartBeatInterval = Integer.parseInt(pro.getProperty("tasktracker.heartbeat.interval"));
		tmpDir = pro.getProperty("mapred.tmp.dir");
	}
	
	public int getPort() {
		return jobTrackerPort;
	}
	
	/* Register a task tracer */
	@Override
	public synchronized TkRegistration register(String address, int port) {
		TaskTrackerInfo newTaskTracker = new TaskTrackerInfo(address, port);
		int id = 0;
		id = nextTaskTrackerId;
		nextTaskTrackerId++;
		taskTrackers.add(newTaskTracker);
		trackerIdToTracker.put(id, newTaskTracker);
		return new TkRegistration(maxMapTasks + maxReduceTasks, id,
								heartBeatInterval, tmpDir);
	}
	
	/* Make a job retired */
	private void retireJob() {
		/* clear data structures 
	     * mapTasksLeft and reduceTasksleft should be zero
	     * reduceTasksToBeRan should be empty
	     * the value set of dnToTaskIds and trackerIdToRunningTaskIds should
	     * be empty
	     */
		nextMapTaskId = 0;
		mapTasks.clear();
		reduceTasks.clear();
		mapTaskIdToDns.clear();
		trackerIdToCompletedMapTaskIds.clear();
		currentJob = null;
		while (jobQueue.isEmpty() == false) {
			currentJob = jobQueue.poll();
			try {
				prepareJob();
				break;
			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	}
	
	/* Generate tasks for current job */
	private void prepareJob() throws RemoteException {
		// get the number of blocks in input file
		TreeMap<Integer, List<DatanodeInfo>> blockToDn =
							namenode.getFileBlocks(currentJob.getInputPath());
		mapTasksLeft = blockToDn.size();
		for (Map.Entry<Integer, List<DatanodeInfo>> entry : blockToDn.entrySet()) {
			int blockId = (int)entry.getKey();
			@SuppressWarnings("unchecked")
			List<DatanodeInfo> datanodes = (List<DatanodeInfo>)entry.getValue();
			// for every block, generate a map task
			MapTask map = new MapTask(nextMapTaskId, blockId, currentJob);
			mapTasks.add(map);
			List<String> belonging = new ArrayList<String>();
			// add this map task to each node which has input file in local
			for (DatanodeInfo datanode : datanodes) {
				String address = datanode.getAddress();
				int port = datanode.getPort();
				belonging.add(address);
				if (dnToMapTaskIds.containsKey(datanode) == false) {
					dnToMapTaskIds.put(address, new HashSet<Integer>());
					addressToPort.put(address, datanode.getPort());
				}
				Set<Integer> taskIds = dnToMapTaskIds.get(datanode);
				taskIds.add(nextMapTaskId);
			}
			// for every map, also record which datanodes it can fetch its input 
			mapTaskIdToDns.put(nextMapTaskId, belonging);
			nextMapTaskId++;
		}
		
		// determine the number of reduce tasks if client doesn't speficy
		reduceTasksLeft = currentJob.getNumReduceTasks();
		if (currentJob.getNumReduceTasks() == 0) {
			reduceTasksLeft = maxReduceTasks * trackerIdToTracker.size();
			currentJob.setNumReduceTasks(reduceTasksLeft);
		}
	}
	
	private void generateReduceTasks() {
		for (int i = 0; i < currentJob.getNumReduceTasks(); ++i) {
			/*
			 * When taskTracker changes in case of failure, the actual object
			 * chages automatically. So no need to change the reference
			 */
			reduceTasks.add(new ReduceTask(i, currentJob, taskTrackers));
		}
	}
	
	/* Select an appropriate map task for this taskTracker */
	private Task selectMapTask(int taskTrackerId) {
		String address = trackerIdToTracker.get(taskTrackerId).getAddress();
		Set<Integer> taskIds = dnToMapTaskIds.get(address);
		Task mapTask = null;
		
		// find a map task whose input file is in this task tracker
		if (taskIds != null) {
			Iterator<Integer> iter = taskIds.iterator();
			mapTask = mapTasks.get(iter.next());
			iter.remove();
		}
		// else just find an available map task
		else {
			for (Map.Entry entry: dnToMapTaskIds.entrySet()) {
				taskIds = (Set<Integer>)entry.getValue();
				if (taskIds.size() > 0) {
					Iterator<Integer> iter = taskIds.iterator();
					mapTask = mapTasks.get(iter.next());
					iter.remove();
					break;
				}
			}
		}
		// remove the id of selected map task from dnToTaskIds 
		if (mapTask != null) {
			int taskId = mapTask.getTaskId();
			List<String> datanodes = mapTaskIdToDns.get(mapTask.getTaskId());
			for (String d : datanodes) {
				dnToMapTaskIds.get(d).remove(taskId);
			}
		}
		return mapTask;
	}
	
	/* Select a reduce task for task tracker */
	private Task selectReduceTask() {
		return reduceTasks.get(reduceTasksToBeRan.remove(0));
	}
	
	/* To do, check the healthy state of task trackers
	 * may use timer?
	 */
	private void checkHealth() {
		
	}
	
	/* Accept job submitted by client */
	@Override
	public synchronized int submitJob(Job job) {
		job.setJobId(nextJobId);
		jobQueue.add(job);
		nextJobId++;
		if (currentJob == null) {
			retireJob();
		}
		return job.getJobId();
	}
	
	@Override
	public synchronized HeartBeatResponse heartBeat(List<Integer> finishedTasks, 
		List<Integer> failedTasks, int numSlots, int taskTrackerId) throws RemoteException {
		/* deal with finished tasks
		 * based on finished tasks, update some data structures
		 * if all the map tasks are done, then generate reducer tasks
		 * if all the reducer tasks are done, then make the current retired
		 * and get new job
		 * 
		 */
		Set<Integer> runningTaskIds = trackerIdToRunningTaskIds.get(taskTrackerId);
		Set<Integer> completedTaskIds = trackerIdToCompletedMapTaskIds.get(taskTrackerId);
		for (int taskId : finishedTasks) {
			runningTaskIds.remove(taskId);
			// then the finished task must be map task
			if (mapTasksLeft > 0) {
				mapTasksLeft--;
				completedTaskIds.add(taskId);
				if (mapTasksLeft == 0)
					generateReduceTasks();
			}
			else {
				reduceTasksLeft--;
				if (reduceTasksLeft == 0)
					retireJob();
			}
		}
		
	    /* deal with failed tasks
		 * if the failed tasks are map tasks, then re-insert the id of these tasks into
		 * dnToTaskIds so that it can be assigned to other task trackers. But we need a blacklist
		 * to record the task trackers that a map task once failed in so that we wouldn't assign
		 * this map task to those task trackers again.
		 *  
		 * if the failed tasks are reduce tasks, then re-insert the id of these tasks into
		 * reduceTasksToBeRan and also add this task trackers to its blacklist.
		 */
		for (int taskId : failedTasks) {
			// then the failed task must be map task
			if (mapTasksLeft > 0) {
				mapTasksLeft++;
				for (String dn : mapTaskIdToDns.get(taskId)) {
					dnToMapTaskIds.get(dn).add(taskId);
				}
				// To do add blacklist
			}
			// then the failed task must be reduce task
			else {
				reduceTasksLeft++;
				reduceTasksToBeRan.add(taskId);
				// To do add blacklist
			}
		}
		/* assign tasks
		 * if numSlots == 0, do nothing
		 * if numSlots > 0
		 *     if currentJob != null assign tasks
		 *     else do nothing
		 */
		if (numSlots == 0 || currentJob == null)
			return new HeartBeatResponse(new ArrayList<Task>());
		
		List<Task> tasksToBeAssigned = new ArrayList<Task>();
		while (numSlots > 0) {
			Task task = null;
			if (mapTasksLeft > 0)
				task = selectMapTask(taskTrackerId);
			else 
				task = selectReduceTask();
			if (task == null)
				break;
			tasksToBeAssigned.add(task);
		}
		return new HeartBeatResponse(tasksToBeAssigned);
	}
	
	@Override
	public synchronized JobProgress checkProgress(int jobId) throws RemoteException {
		return new JobProgress(mapTasks.size(), currentJob.getNumReduceTasks(),
							mapTasksLeft, reduceTasksLeft);
	}
	
	public static void main(String[] args) throws Exception {
		JobTracker jobTracker = new JobTracker();
		jobTracker.loadConfiguration();
		JobTrackerProtocol jobTrackerStub = 
				(JobTrackerProtocol)UnicastRemoteObject.exportObject(jobTracker, 0);
		Registry registry = LocateRegistry.createRegistry(jobTracker.getPort());
		registry.rebind(JOBTRACKER_RMI_NAME, jobTrackerStub);
	}	
}
