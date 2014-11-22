package jobtracker;

import java.io.Serializable;

public class TaskTrackerInfo implements Serializable{
	private String address;
	private int port;
	private long lastHeartBeatTime;
	private TaskTrackerStatus status;
	
	public TaskTrackerInfo(String address, int port) {
		this.address = address;
		this.port = port;
		this.status = TaskTrackerStatus.ALIVE;
	}
	
	public String getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}
	
	public long getLastHeartBeatTime() {
		return lastHeartBeatTime;
	}
	
	public void updateHeartBeatTime(long heartBeatTime) {
		this.lastHeartBeatTime = heartBeatTime;
	}
}
