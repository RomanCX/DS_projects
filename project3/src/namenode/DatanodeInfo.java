package namenode;

import java.io.Serializable;

public class DatanodeInfo implements Serializable {
	private String address;
	private int port;
	private int id;
	private long lastHeartbeatTime;
	private DatanodeStatus status;
	
	public static final int DEFAULT_TIME_OUT_INTERVAL = 10000; //ms
	
	public DatanodeInfo(String address, int port, int id) {
		this.address = address;
		this.port = port;
		this.id = id;
		this.lastHeartbeatTime = System.currentTimeMillis();
		this.status = DatanodeStatus.ALIVE;
	}
	
	public String getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}
	
	public boolean isAlive() {
		return status == DatanodeStatus.ALIVE;
	}

	public DatanodeStatus getStatus() {
		return status;
	}
	
	public DatanodeStatus checkHealth() {
		if (System.currentTimeMillis() - lastHeartbeatTime >= DEFAULT_TIME_OUT_INTERVAL) {
			status = DatanodeStatus.DEAD;
		}
		return status;
	}
	
	public void heartBeat() {
		status = DatanodeStatus.ALIVE;
		lastHeartbeatTime = System.currentTimeMillis();
	}
	
	public int getId() {
		return id;
	}
}
