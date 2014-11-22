package mapredCommon;

import java.io.Serializable;

public class Job implements Serializable {
	private String inputPath;
	private String outputPath;
	private String jarFile;
	private int jobId;
	private String delim;
	private int numReduceTasks;
	private Class<Mapper> mapClass;
	private Class<Reducer> reduceClass;
	
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
		if (!outputPath.endsWith("/")) {
			outputPath += "/";
		}
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
	
	public Class<Mapper> getMapClass() {
		return mapClass;
	}
	
	public Class<Reducer> getReduceClass() {
		return reduceClass;
	}
	
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
	
	public void setNumReduceTasks(int numReduceTasks) {
		this.numReduceTasks = numReduceTasks;
	}
	
	public void setMapClass(Class<Mapper> mapClass) {
		this.mapClass = mapClass;
	}
	
	public void setReduceClass(Class<Reducer> reduceClass) {
		this.reduceClass = reduceClass;
	}
	
	public void setDelim(String delim) {
		this.delim = delim;
	}
	
	public void setJarFile(String jarFile) {
		this.jarFile = jarFile;
	}
	
	public void setInputPath(String inputPath) {
		this.inputPath = inputPath;
	}
	
	public void setOUtputPath(String outputPath) {
		this.outputPath = outputPath;
	}
}
	
