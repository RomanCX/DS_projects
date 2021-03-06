package datanode;

import java.io.Serializable;

public class Block implements Serializable {
	private String data; // now we assume the data is string
	private int id;
	
	public Block() {
		
	}
	
	public Block(Integer id, String data) {
		this.id = id;
		this.data = data;
	}
	
	public String getData() {
		return data;
	}
	
	public int getId() {
		return id;
	}
}
