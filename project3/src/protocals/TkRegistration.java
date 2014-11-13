package protocals;

public class TkRegistration {
	private int maxAllowedJobs;
	private int taskTrackerId;
	private int interval;
	
	public TkRegistration(int maxAllowedJobs, int taskTrackerId, int interval) {
		this.maxAllowedJobs = maxAllowedJobs;
		this.taskTrackerId = taskTrackerId;
		this.interval = interval;
	}
	
	public int getMaxAllowedJobs() {
		return maxAllowedJobs;
	}
	
	public int getTaskTrackerId() {
		return taskTrackerId;
	}
	
	public int getInterval() {
		return interval;
	}

}
