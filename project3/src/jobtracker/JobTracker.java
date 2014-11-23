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
import protocals.TaskTrackerOperation;
import protocals.TkRegistration;

public class JobTracker implements JobTrackerProtocol {
	
	// some constants
	private static final String JOBTRACKER_RMI_NAME = "jobtracker";
	private static final String MAPRED_CONFIGURE_FILE="../conf/mapred.cnf";
	private static final String DFS_CONFIGURE_FILE="../conf/dfs.cnf";
		
	// port number of jobTracker
	private int jobTrackerPort;
	// number of tasks which can be executed in a  tasktraceker simultaneously
	private int maxTasks;
	// interval for heart beat
	private int heartBeatInterval;
	// threshold for consecutive timeout
	private int threshold;
	// directory for storing inputs and outputs of tasks
	private String tmpDir;
	// port number of namenode
	private int namenodePort;
	// protocol of name node
	private NamenodeProtocal namenode;
	
	// flag for shutting down
	boolean toShutDown;
	// number of shutting down notifications that need to send
	private int numNotification;
	// next available job id
	private int nextJobId;
	// next available taskTracker id
	private int nextTaskTrackerId;
	// next available map task id
	private int nextMapTaskId;
	// the job being executed
	private Job currentJob;
	// a queue storing jobs to be executed
	private Queue<Job> jobQueue;
	// progress for each job
	private HashMap<Integer, JobProgress> jobProgresses;
	// list of taskTrackers
	List<TaskTrackerInfo> taskTrackers;
	// trakcerId -> taskTracker
	protected HashMap<Integer, TaskTrackerInfo> trackerIdToTracker;
	// map tasks of current job
	private List<MapTask> mapTasks;
	// black list for map task
	private List<Set<Integer>> blacklistOfMap;
	// reduce tasks of current job
	private List<ReduceTask> reduceTasks;
	// black list for reduce task
	private List<Set<Integer>> blacklistOfReduce; 
	// reduce tasks to be ran
	private List<Integer> reduceTasksToBeRan;
	// address -> list of taskId
	private TreeMap<String, Set<Integer>> dnToMapTaskIds;
	// taskId -> list of address
	private HashMap<Integer, List<String>> mapTaskIdToDns;
	// address of data node -> port of data node
	private HashMap<String, Integer> addressToPort;
	// task tracker -> list of id of running tasks
	private HashMap<Integer, Set<Integer>> trackerIdToRunningTaskIds;
	// task tracker -> list of id of completed tasks
	private HashMap<Integer, Set<Integer>> trackerIdToCompletedMapTaskIds;
	
	public JobTracker() {
		toShutDown = false;
		nextJobId = 1;
		nextTaskTrackerId = 0;
		currentJob = null;
		jobQueue = new LinkedList<Job>();
		jobProgresses = new HashMap<Integer, JobProgress>();
		taskTrackers = new ArrayList<TaskTrackerInfo>();
		trackerIdToTracker = new HashMap<Integer, TaskTrackerInfo>();
		mapTasks = new ArrayList<MapTask>();
		blacklistOfMap = new ArrayList<Set<Integer>>();
		reduceTasks = new ArrayList<ReduceTask>();
		blacklistOfReduce = new ArrayList<Set<Integer>>();
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
		String name = pro.getProperty("fs.default.name", "dfs://localhost:1099");
		int pos1 = name.indexOf("//");
		int pos2 = name.indexOf(":", pos1 + 2);
		String namenodeAddr = name.substring(pos1 + 2, pos2);
		namenodePort = Integer.parseInt(name.substring(pos2 + 1));
		Registry registry = LocateRegistry.getRegistry(namenodeAddr, namenodePort);
		namenode = (NamenodeProtocal)registry.lookup("namenode");
			
		// load configuration for job tracker
		pro.clear();
		pro.load(new FileReader(MAPRED_CONFIGURE_FILE));
		jobTrackerPort = Integer.parseInt(pro.getProperty("jobtracker.port", "2099"));
		maxTasks = 
				Integer.parseInt(pro.getProperty("tasktracker.tasks.maximum", "2"));
		heartBeatInterval = 
				Integer.parseInt(pro.getProperty("tasktracker.heartbeat.interval", "3000"));
		tmpDir = pro.getProperty("mapred.tmp.dir", "tmp/mapred");
		threshold = Integer.parseInt(pro.getProperty("tasktracker.timeout.threshold", "2"));
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
		trackerIdToRunningTaskIds.put(id, new HashSet<Integer>());
		trackerIdToCompletedMapTaskIds.put(id, new HashSet<Integer>());
		// start a thread to monitor the heath state of this tasktracker
		new Thread(new HealthMonitor(this, id, heartBeatInterval, threshold)).start();
		return new TkRegistration(maxTasks, id,
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
		blacklistOfMap.clear();
		reduceTasks.clear();
		blacklistOfReduce.clear();
		mapTaskIdToDns.clear();
		for (Set<Integer> tasks : trackerIdToCompletedMapTaskIds.values()) {
			tasks.clear();
		}
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
		// get all the files in this input path
		List<String> files = namenode.ls(currentJob.getInputPath());
		// for each file, generate map tasks
		for (String file : files) {
			// get the number of blocks in input file
			TreeMap<Integer, List<DatanodeInfo>> blockToDn =
					namenode.getFileBlocks(file);
			// for each block, generate a map task
			for (Map.Entry<Integer, List<DatanodeInfo>> entry : blockToDn.entrySet()) {
				int blockId = (int)entry.getKey();
				@SuppressWarnings("unchecked")
				List<DatanodeInfo> datanodes = (List<DatanodeInfo>)entry.getValue();
				MapTask map = new MapTask(nextMapTaskId, blockId, currentJob);
				mapTasks.add(map);
				blacklistOfMap.add(new HashSet<Integer>());
				List<String> belonging = new ArrayList<String>();
				// add this map task to every node which has input file in local
				for (DatanodeInfo datanode : datanodes) {
					String address = datanode.getAddress();
					int port = datanode.getPort();
					belonging.add(address);
					if (dnToMapTaskIds.containsKey(address) == false) {
						dnToMapTaskIds.put(address, new HashSet<Integer>());
						addressToPort.put(address, datanode.getPort());
					}
					Set<Integer> taskIds = dnToMapTaskIds.get(address);
					taskIds.add(nextMapTaskId);
				}
				// for every map, also record which datanodes it can fetch its input 
				mapTaskIdToDns.put(nextMapTaskId, belonging);
				nextMapTaskId++;
			}
		}
		
		// determine the number of reduce tasks if client doesn't specify
		int reduceTasksNum = currentJob.getNumReduceTasks();
		if (currentJob.getNumReduceTasks() == 0) {
			reduceTasksNum = maxTasks * trackerIdToTracker.size();
			currentJob.setNumReduceTasks(reduceTasksNum);
		}
		
		// create progress for this job
		JobProgress progress = new JobProgress(mapTasks.size(), reduceTasksNum,
											mapTasks.size(), reduceTasksNum);
		jobProgresses.put(currentJob.getJobId(), progress);
		System.out.println("JobProgress is " + jobProgresses);
	}
	
	/* Generate reduce tasks for current job */
	private void generateReduceTasks() {
		System.out.println("number of reduceTasks: " + currentJob.getNumReduceTasks());
		for (int i = 0; i < currentJob.getNumReduceTasks(); ++i) {
			/*
			 * When taskTracker changes in case of failure, the actual object
			 * changes automatically. So no need to change the reference
			 */
			reduceTasks.add(new ReduceTask(i, currentJob, taskTrackers, namenodePort));
			reduceTasksToBeRan.add(i);
			blacklistOfReduce.add(new HashSet<Integer>());
		}
		System.out.println("reduceTasksToBeRan" + reduceTasksToBeRan);
	}
	
	/* Select an appropriate map task for this taskTracker */
	private MapTask selectMapTask(int taskTrackerId) {
		String address = trackerIdToTracker.get(taskTrackerId).getAddress();
		Set<Integer> taskIds = dnToMapTaskIds.get(address);
		MapTask mapTask = null;
		System.out.println("tasktracker address is " + address);
		// find a map task whose input file is in this task tracker
		if (taskIds != null && taskIds.size() > 0) {
			Iterator<Integer> iter = taskIds.iterator();
			mapTask = mapTasks.get(iter.next());
			mapTask.setDatanode(new DatanodeInfo(address, addressToPort.get(address)));
			iter.remove();
		}
		// else just find an available map task
		else {
			boolean flag = false;
			for (Map.Entry entry: dnToMapTaskIds.entrySet()) {
				taskIds = (Set<Integer>)entry.getValue();
				if (taskIds.size() > 0) {
					Iterator<Integer> iter = taskIds.iterator();
					while (iter.hasNext()) {
						int id = iter.next();
						if (blacklistOfMap.get(id).contains(taskTrackerId))
							continue;
						mapTask = mapTasks.get(id);
						address = (String)entry.getKey();
						mapTask.setDatanode(new DatanodeInfo(address, 
											addressToPort.get(address)));
						iter.remove();
						flag = true;
						break;
					}
					if (flag == true)
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
	private ReduceTask selectReduceTask(int taskTrackerId) {
		// for every reduce task, if taskTrackerId is not in its blacklist
		// then this task will be assigned
		for (int i = 0; i < reduceTasksToBeRan.size(); ++i) {
			int id = reduceTasksToBeRan.get(i);
			if (blacklistOfReduce.get(id).contains(taskTrackerId))
				continue;
			reduceTasksToBeRan.remove(i);
			return reduceTasks.get(id);
		}
		return null;
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
		synchronized(trackerIdToTracker) {
			/* update the lastHeartBeatTime of this tasktracker */
			trackerIdToTracker.get(taskTrackerId).
									updateHeartBeatTime(System.currentTimeMillis());
		}
		if (toShutDown == true) {
			numNotification--;
			return new HeartBeatResponse(new ArrayList<Task>(), 
									TaskTrackerOperation.SHUT_DOWN);
		}
		
		if (currentJob == null)
			return new HeartBeatResponse(new ArrayList<Task>(),
									TaskTrackerOperation.RUN_TASK);
		
		/* deal with finished tasks
		 * based on finished tasks, update some data structures
		 * if all the map tasks are done, then generate reducer tasks
		 * if all the reducer tasks are done, then make the current retired
		 * and get new job
		 * 
		 */
		System.out.println("finished tasks" + finishedTasks);
		Set<Integer> runningTaskIds = trackerIdToRunningTaskIds.get(taskTrackerId);
		Set<Integer> completedTaskIds = trackerIdToCompletedMapTaskIds.get(taskTrackerId);
		System.out.println("completed tasks " + completedTaskIds);
		
		System.out.println("job id " + currentJob.getJobId());
		// get the job progress
		JobProgress progress = jobProgresses.get(currentJob.getJobId());
		int mapTasksLeft = progress.getMapTasksLeft();
		int reduceTasksLeft = progress.getReduceTasksLeft();
		for (int taskId : finishedTasks) {
			runningTaskIds.remove(taskId);
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
		String trackerAddress = taskTrackers.get(taskTrackerId).getAddress();
		for (int taskId : failedTasks) {
			// then the failed task must be map task
			if (mapTasksLeft > 0) {
				mapTasksLeft++;
				for (String dn : mapTaskIdToDns.get(taskId)) {
					// don't add this map task to this tasktracker again
					// because it has failed on this tasktracker
					if (dn == trackerAddress)
						continue;
					dnToMapTaskIds.get(dn).add(taskId);
				}
				// also add this tasktracker to the blacklist so that this map task
				// won't be executed in this tasktracker again
				blacklistOfMap.get(taskId).add(taskTrackerId);
			}
			// then the failed task must be reduce task
			else {
				reduceTasksLeft++;
				reduceTasksToBeRan.add(taskId);
				blacklistOfReduce.get(taskId).add(taskTrackerId);
			}
		}
		// update the job progress
		progress.setMapTasksLeft(mapTasksLeft);
		progress.setReduceTasksLeft(reduceTasksLeft);
		
		/* assign tasks
		 * if numSlots == 0, do nothing
		 * if numSlots > 0
		 *     if currentJob != null assign tasks
		 *     else do nothing
		 */
		if (numSlots == 0 || currentJob == null)
			return new HeartBeatResponse(new ArrayList<Task>(),
									TaskTrackerOperation.RUN_TASK);
		
		System.out.println("numSlots: " + numSlots);
		System.out.println("mapTasksLeft in heartbeat: " + progress.getMapTasksLeft());
		List<Task> tasksToBeAssigned = new ArrayList<Task>();
		while (numSlots > 0) {
			Task task = null;
			if (progress.getMapTasksLeft() > 0)
				task = selectMapTask(taskTrackerId);
			else 
				task = selectReduceTask(taskTrackerId);
			if (task == null)
				break;
			tasksToBeAssigned.add(task);
			trackerIdToRunningTaskIds.get(taskTrackerId).add(task.getTaskId());
			numSlots--;
		}
		return new HeartBeatResponse(tasksToBeAssigned,
								TaskTrackerOperation.RUN_TASK);
	}
	
	@Override
	public synchronized JobProgress checkProgress(int jobId) throws RemoteException {
		return jobProgresses.get(jobId);
	}
	
	public synchronized void removeFailedTracker(int taskTrackerId) {
		try {
			JobProgress progress = jobProgresses.get(currentJob.getJobId());
			int mapTasksLeft = progress.getMapTasksLeft();
			int reduceTasksLeft = progress.getReduceTasksLeft();
			// running tasks (map or reduce)
			for (int taskId : trackerIdToRunningTaskIds.get(taskTrackerId)) {
				if (mapTasksLeft > 0) {
					for (String dn : mapTaskIdToDns.get(taskId)) {
						dnToMapTaskIds.get(dn).add(taskId);
					}
				}
				else {
					reduceTasksToBeRan.add(taskId);
					reduceTasksLeft++;
				}
			}
			// completed tasks (only map)
			for (int taskId : trackerIdToCompletedMapTaskIds.get(taskTrackerId)) {
				for (String dn : mapTaskIdToDns.get(taskId)) {
					dnToMapTaskIds.get(dn).add(taskId);
				}
				mapTasksLeft++;
			}
			
			//System.out.println("mapTasksLeft after remove tracker " + mapTasksLeft);
			
			// update progress
			progress.setMapTasksLeft(mapTasksLeft);
			progress.setReduceTasksLeft(reduceTasksLeft);
		} catch (Exception e) {
			// currentJob may be null, the there will be null pointer exception
			// and we choose to ignore it
		}
		
		// remove it from taskTrackers, trackerIdToTracker, trackerIdToRunningTaskIds
		// trackerIdToCompletedMapTaskIds
		// when it recovers, it will receive a new tasktracker id, so the blacklist doesn't
		// need to change
		TaskTrackerInfo tracker = trackerIdToTracker.get(taskTrackerId);
		synchronized(taskTrackers) {
			taskTrackers.remove(tracker);
		}
		trackerIdToTracker.remove(taskTrackerId);
		trackerIdToRunningTaskIds.remove(taskTrackerId);
		trackerIdToCompletedMapTaskIds.remove(taskTrackerId);	
	}
	
	public static void main(String[] args) throws Exception {
		JobTracker jobTracker = new JobTracker();
		jobTracker.loadConfiguration();
		JobTrackerProtocol jobTrackerStub = 
				(JobTrackerProtocol)UnicastRemoteObject.exportObject(jobTracker, 0);
		Registry registry = LocateRegistry.createRegistry(jobTracker.getPort());
		registry.rebind(JOBTRACKER_RMI_NAME, jobTrackerStub);
	}

	@Override
	public void shutdown() throws RemoteException {
		toShutDown = true;
		synchronized(taskTrackers) {
			numNotification = taskTrackers.size();
		}
		while (numNotification > 0) {
			try {
				Thread.sleep(heartBeatInterval);
			} catch (InterruptedException e) {
				//Do nothing
			}
		}
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				try {
					System.out.println("System going to shut down in 5 seconds");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					//Do nothing
				}
				System.exit(0);
			}
		}).start();
	}	
}

/* 
 * Check the health state of tasktracker
 */
class HealthMonitor implements Runnable {
	private JobTracker jobTracker;
	private int taskTrackerId;
	private long heartBeatInterval;
	private long lastCheck;
	private int timeoutCount;
	private int threshold;
	
	public HealthMonitor(JobTracker jobTracker, int taskTrackerId,
					long heartBeatInterval, int threshold) {
		this.jobTracker = jobTracker;
		this.taskTrackerId = taskTrackerId;
		this.heartBeatInterval = heartBeatInterval;
		this.timeoutCount = 0;
		this.threshold = threshold;
	}
	
	public void run() {
		while (true) {
			if (jobTracker.toShutDown == true)
				break;
			long lastHeartBeatTime = 0;
			synchronized(jobTracker.trackerIdToTracker) {
				TaskTrackerInfo tracker = 
						jobTracker.trackerIdToTracker.get(taskTrackerId);
				lastHeartBeatTime = tracker.getLastHeartBeatTime();
			}
			long now = System.currentTimeMillis();
			if (now - lastHeartBeatTime > heartBeatInterval) {
				timeoutCount++;
				// if one tasktracker has some number of consecutive timeout
				// then consider it a failed tasktracker and remove it
				if (timeoutCount > threshold) {
					System.out.println("tasktracker " + taskTrackerId + " fails");
					jobTracker.removeFailedTracker(taskTrackerId);
					break;
				}
			}
			else {
				timeoutCount = 0;
			}
			
			lastCheck = now;
			long timeLeft = heartBeatInterval;
			while (timeLeft > 0) {
				try {
					Thread.sleep(timeLeft);
				} catch (InterruptedException e) {
					// ignore
				}
				timeLeft = lastCheck + heartBeatInterval - System.currentTimeMillis();
			}
		}
	}
	
}
