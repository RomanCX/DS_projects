package tasktracker;

import java.io.FileReader;
import java.io.IOException;
import java.net.Inet4Address;
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

import protocals.HeartBeatResponse;
import protocals.JobTrackerProtocol;
import protocals.TkRegistration;
import mapredCommon.Task;

public class TaskTracker {
	int maxAllowedTasks;
	long lastHeartBeatTime;
	HashMap<Integer, Task> runningTasks;
	List<Integer> finishedTasks;
	Registry registry;
	String jobTrackerAddress;
	int jobTrackerPort;
	JobTrackerProtocol jobTracker;
	String mapOutputDir;
	String myAddress;
	int myPort;
	int id;
	int heartBeatInterval;
	
	static final String CONFIG_FILE_NAME = "tasktracker.cnf";
	static final String DEFAULT_MAP_OUTPUT_DIR = "mapOutput";
	static final String JOB_TRACKER_NAME = "jobtracker";
	
	public TaskTracker() {
		int numProcessors = Runtime.getRuntime().availableProcessors();
		this.runningTasks = new HashMap<Integer, Task>();
		this.finishedTasks = new ArrayList<Integer>();
		lastHeartBeatTime = 0;
		loadConfiguration();
		
		try {
			registry = LocateRegistry.getRegistry(jobTrackerAddress, jobTrackerPort);
			jobTracker = (JobTrackerProtocol) registry.lookup(JOB_TRACKER_NAME);
			myAddress = Inet4Address.getLocalHost().getHostName();
			myPort = getFreePort();
			TkRegistration tkRegistration = jobTracker.register(myAddress, myPort);
			heartBeatInterval = tkRegistration.getInterval();
			maxAllowedTasks = tkRegistration.getMaxAllowedTasks();
			if (numProcessors < maxAllowedTasks) {
				maxAllowedTasks = numProcessors;
			}
			id = tkRegistration.getTaskTrackerId();
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
	
	
	public static int getFreePort() throws IOException {
		ServerSocket tmp = new ServerSocket(0);
		int myPort = tmp.getLocalPort();
		tmp.close();
		return myPort;
	}
	private void loadConfiguration() {
		Properties pro = new Properties();
		try {
			pro.load(new FileReader(CONFIG_FILE_NAME));
			// assume the format of master.default.name is hostname:port
			String name = pro.getProperty("master.default.name");
			int posColon = name.indexOf(":");
			jobTrackerAddress = name.substring(0, posColon);
			jobTrackerPort = Integer.parseInt(name.substring(posColon + 1));
			
			// format name and datadir
			mapOutputDir = pro.getProperty("mr.mapoutput.dir", DEFAULT_MAP_OUTPUT_DIR);
			
			System.out.println("Master address: " + name);
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
					lastHeartBeatTime = currTime;
					HeartBeatResponse heartBeatResponse =
							jobTracker.heartBeat(finishedTasks, numSlots, id);
					parseHeartBeatResponse(heartBeatResponse);
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
		//TODO: not implementedf
	}
	
	public static void main(String[] args) {
		TaskTracker taskTracker = new TaskTracker();
		taskTracker.run();
	}
	
}
