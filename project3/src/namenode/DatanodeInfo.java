package namenode;

public class DatanodeInfo {
	private String address;
	private int port;
	private int id;
	public DatanodeInfo(String address, int port, int id) {
		this.address = address;
		this.port = port;
		this.id = id;
	}
	public String getAddress() {
		return address;
	}
	public int getPort() {
		return port;
	}
}
