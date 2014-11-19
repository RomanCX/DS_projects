package protocals;

import java.io.Serializable;
import java.util.List;

import mapredCommon.Task;

public class HeartBeatResponse implements Serializable  {
	private List<Task> tasks;
	
	public HeartBeatResponse(List<Task> tasks) {
		this.tasks = tasks;
	}
	
	public List<Task> getTasks() {
		return tasks;
	}
}
