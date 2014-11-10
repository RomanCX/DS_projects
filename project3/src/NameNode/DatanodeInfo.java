package namenode;

public class DatanodeInfo {
	private String address;
	private int port;
	public DatanodeInfo(String address, int port) {
		this.address = address;
		this.port = port;
	}
	public String getAddress() {
		return address;
	}
	public int getPort() {
		return port;
	}
}
