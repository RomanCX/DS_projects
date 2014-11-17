package mapredCommon;

public class OutputPath {
	Job job;
	int taskId;
	int reduceId;
	
	public OutputPath(Job job, int taskId, int reduceId) {
		this.job = job;
		this.taskId = taskId;
		this.reduceId = reduceId;
	}
	
	public String getMapPath(String outputDir) {
		return outputDir + "/map" + job.getJobId() + "_" + taskId + "_" + reduceId;
	}
	
	String getReduceSortedPath(String outputDir) {
		return outputDir + "/reduce_tmp" + job.getJobId() + "_" + taskId;

	}
	
	public String getReduceTmpPath(String outputDir, int taskTrackerIndex, int outputIndex) {
		return getReduceSortedPath(outputDir) + "_" + taskTrackerIndex + "_" + outputIndex;
	}
	
	public String getReducePath(String outputDir) {
		return outputDir + "/reduce" + job.getJobId() + "_" + taskId;
	}
}

