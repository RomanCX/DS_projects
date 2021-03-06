package jobtracker;

import java.io.Serializable;

public class JobProgress implements Serializable {
	private int numMapTasks;
	private int numReduceTasks;
	private int mapTasksLeft;
	private int reduceTasksLeft;
	
	public JobProgress(int numMapTasks, int numReduceTasks, int mapTasksLeft,
					int reduceTasksLeft) {
		this.numMapTasks = numMapTasks;
		this.numReduceTasks = numReduceTasks;
		this.mapTasksLeft = mapTasksLeft;
		this.reduceTasksLeft = reduceTasksLeft;
	}
	
	public double getMapProgress() {
		return 1.0  - 1.0 * mapTasksLeft / numMapTasks;
	}
	
	public double getReduceProgress() {
		return 1.0 - 1.0 * reduceTasksLeft / numReduceTasks;
	}
	
	public boolean isFinished() {
		return reduceTasksLeft == 0;
	}
	
	public int getMapTasksLeft() {
		return mapTasksLeft;
	}
	
	public int getReduceTasksLeft() {
		return reduceTasksLeft;
	}
	
	public void setMapTasksLeft(int mapTasksLeft) {
		this.mapTasksLeft = mapTasksLeft;
	}
	
	public void setReduceTasksLeft(int reduceTasksLeft) {
		this.reduceTasksLeft = reduceTasksLeft;
	}
}
