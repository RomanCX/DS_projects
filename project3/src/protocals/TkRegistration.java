package protocals;

public class TkRegistration {
	private int maxAllowedTasks;
	private int taskTrackerId;
	private int interval;
	private String tmpDir;
	
	public TkRegistration(int maxAllowedTasks, int taskTrackerId, int interval,
						String tmpDir) {
		this.maxAllowedTasks = maxAllowedTasks;
		this.taskTrackerId = taskTrackerId;
		this.interval = interval;
		this.tmpDir = tmpDir;
	}
	
	public int getMaxAllowedTasks() {
		return maxAllowedTasks;
	}
	
	public int getTaskTrackerId() {
		return taskTrackerId;
	}
	
	public int getInterval() {
		return interval;
	}
	
	public String getTmpDir() {
		return tmpDir;
	}
}
