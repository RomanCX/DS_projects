package mapredCommon;

import java.io.Serializable;

public class Job implements Serializable {
	private String inputPath;
	private String outputPath;
	private String jarFile;
	private int jobId;
	private String delim;
	private int numReduceTasks;
	
	public Job(String inputPath, String outputPath, String jarFile, 
			String delim, int numReduceTasks) {
		this.inputPath = inputPath;
		this.outputPath = outputPath;
		this.jarFile = jarFile;
		this.delim = delim;
		this.numReduceTasks = numReduceTasks;
	}
	
	public Job(String inputPath, String outputPath, String jarFile) {
		this(inputPath, outputPath, jarFile, "\t", 0);
	}
	
	public Job(String inputPath, String outputPath, String jarFile, String delim) {
		this(inputPath, outputPath, jarFile, delim, 0);
	}
	
	public Job(String inputPath, String outputPath, String jarFile,
			int numReduceTasks) {
		this(inputPath, outputPath, jarFile, "\t", numReduceTasks);
	}
	
	public String getInputPath() {
		return inputPath;
	}
	
	public String getOutputPath() {
		return outputPath;
	}
	
	public String getJarFile() {
		return jarFile;
	}
	
	public int getJobId() {
		return jobId;
	}
	
	public String getDelim() {
		return delim;
	}
	
	public int getNumReduceTasks() {
		return numReduceTasks;
	}
	
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
	
	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}
}
