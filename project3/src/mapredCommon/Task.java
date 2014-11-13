package mapredCommon;

import java.io.Serializable;

public class Task implements Serializable {
	int id;
	String jarFileName;
	
	public Task(int id, String jarFileName) {
		this.id = id;
		this.jarFileName = jarFileName;
	}
}
