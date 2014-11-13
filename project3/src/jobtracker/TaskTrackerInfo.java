package jobtracker;

public class TaskTrackerInfo {
	private String address;
	private int port;
	private long lastHeartBeatTime;
	private TaskTrackerStatus status;
	
	public TaskTrackerInfo(String address, int port) {
		this.address = address;
		this.port = port;
		this.status = TaskTrackerStatus.ALIVE;
	}
	
}
