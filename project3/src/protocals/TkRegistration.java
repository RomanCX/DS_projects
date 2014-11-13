package protocals;

public class TkRegistration {
	private int maxAllowedTasks;
	private int taskTrackerId;
	private int interval;
	
	public TkRegistration(int maxAllowedTasks, int taskTrackerId, int interval) {
		this.maxAllowedTasks = maxAllowedTasks;
		this.taskTrackerId = taskTrackerId;
		this.interval = interval;
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

}
