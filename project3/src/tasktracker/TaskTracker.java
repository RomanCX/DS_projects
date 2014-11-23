package tasktracker;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.jar.JarFile;

import protocals.HeartBeatResponse;
import protocals.JobTrackerProtocol;
import protocals.TaskTrackerOperation;
import protocals.TkRegistration;
import mapredCommon.MapTask;
import mapredCommon.Task;

public class TaskTracker {
	int maxAllowedTasks;
	long lastHeartBeatTime;
	HashMap<Integer, Task> runningTasks;
	List<Integer> failedTasks;
	List<Integer> finishedTasks;
	Registry registry;
	String jobTrackerAddress;
	int jobTrackerPort;
	JobTrackerProtocol jobTracker;
	String mapOutputDir;
	ServerSocket listenerSocket;
	int id;
	int heartBeatInterval;
	
	static TaskTracker taskTracker= null;
	
	static final String CONFIG_FILE_NAME = "../conf/mapred.cnf";
	static final String DEFAULT_MAP_OUTPUT_DIR = "mapOutput";
	static final String JOB_TRACKER_NAME = "jobtracker";
	
	//Marked private to enforce singleton
	private TaskTracker() {
		int numProcessors = Runtime.getRuntime().availableProcessors();
		this.runningTasks = new HashMap<Integer, Task>();
		this.finishedTasks = new ArrayList<Integer>();
		this.failedTasks = new ArrayList<Integer>();
		lastHeartBeatTime = 0;
		loadConfiguration();
		
		try {
			registry = LocateRegistry.getRegistry(jobTrackerAddress, jobTrackerPort);
			jobTracker = (JobTrackerProtocol) registry.lookup(JOB_TRACKER_NAME);
			listenerSocket = new ServerSocket(0);
			TkRegistration tkRegistration = jobTracker.register(InetAddress.getLocalHost().getHostAddress(), listenerSocket.getLocalPort());
			setMapOutputDir(tkRegistration.getTmpDir());
			heartBeatInterval = tkRegistration.getInterval();
			maxAllowedTasks = tkRegistration.getMaxAllowedTasks();
			if (numProcessors < maxAllowedTasks) {
				maxAllowedTasks = numProcessors;
			}
			id = tkRegistration.getTaskTrackerId();
			TaskTrackerListener listener = new TaskTrackerListener(listenerSocket);
			new Thread(listener).start();
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static TaskTracker getInstance() {
		if (taskTracker == null) {
			taskTracker = new TaskTracker();
		}
		return taskTracker;
	}
	
	
	private void loadConfiguration() {
		Properties pro = new Properties();
		try {
			pro.load(new FileReader(CONFIG_FILE_NAME));
			// assume the format of master.default.name is hostname:port
			String fsName = pro.getProperty("fs.default.name");
			int posColon = fsName.indexOf(":");
			jobTrackerAddress = fsName.substring(0, posColon);
			jobTrackerPort = Integer.parseInt(pro.getProperty("jobtracker.port"));
			
			System.out.println("Master address: " + fsName + ":" + jobTrackerPort);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	public void run() {
		while (true) {
			long currTime = System.currentTimeMillis();
			if (currTime - lastHeartBeatTime > heartBeatInterval) {
				int numSlots;
				synchronized (runningTasks) {
					numSlots = maxAllowedTasks - runningTasks.size();
				}
				try {
					synchronized (finishedTasks) {
						synchronized (failedTasks) {
							lastHeartBeatTime = currTime;
							HeartBeatResponse heartBeatResponse =
									jobTracker.heartBeat(finishedTasks, failedTasks, numSlots, id);
							finishedTasks.clear();
							parseHeartBeatResponse(heartBeatResponse);
						}
					}
				} catch (RemoteException e) {
					System.err.println("Failed to connect to Job Tracker");
					e.printStackTrace();
				}
				
			} else {
				try {
					Thread.sleep(currTime + heartBeatInterval - lastHeartBeatTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
			}
		}
	}
	
	private void parseHeartBeatResponse(HeartBeatResponse heartBeatResponse) {
		if (heartBeatResponse.getOperation() == TaskTrackerOperation.SHUT_DOWN) {
			System.exit(0);
		}
		List<Task> newTasks = heartBeatResponse.getTasks();
		for (Task newTask : newTasks) {
			try {
				newTask.setTmpDir(mapOutputDir);
				new Thread(newTask).start();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void reportFinished(int taskId) {
		synchronized (finishedTasks) {
			System.out.println("Task " + taskId + " finished.");
			runningTasks.remove(taskId);
			finishedTasks.add(taskId);
		}
	}
	
	public void reportFailed(int taskId) {
		synchronized (failedTasks) {
			failedTasks.add(taskId);
			runningTasks.remove(taskId);
		}
	}
	
	public static void main(String[] args) {
		TaskTracker myTaskTracker = TaskTracker.getInstance();
		myTaskTracker.run();
	}
	
	public String getMapOutputDir() {
		return mapOutputDir;
	}

	public String getJobTrackerAddress() {
		return jobTrackerAddress;
	}
	
	public int getJobTrackerPort() {
		return jobTrackerPort;
	}
	
	private void setMapOutputDir(String tmpDir) {
		if (tmpDir.endsWith("/")) {
			mapOutputDir = tmpDir.substring(0, tmpDir.length() - 1);
		}
		File dir = new File(tmpDir);
		dir.mkdirs();
		
	}
	
	
}
