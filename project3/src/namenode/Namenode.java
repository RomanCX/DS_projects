package namenode;
import java.util.HashMap;
import java.util.List;


public class Namenode {
	private HashMap<Integer, DatanodeInfo> dataNodes;
	private HashMap<String, List<Integer> > files;
	private HashMap<Integer, List<Integer> > blocks;
	private int dataNodeCount;
	
	public static final int DEFAULT_HEARTBEAT_INTERVAL = 60;
	
	
	public Namenode() {
		dataNodes = new HashMap<Integer, DatanodeInfo>();//<datanodeId, datanodeInfo>
		files = new HashMap<String, List<Integer>>();//<filename, blockIds>
		blocks = new HashMap<Integer, List<Integer>>();	//<blockId, datanodeIds> 
		dataNodeCount = 0;
	}
	
	public int addDatanode(String address, int port) {
		DatanodeInfo newDatanode = new DatanodeInfo(address, port);
		dataNodes.put(dataNodeCount, newDatanode);
		dataNodeCount++;
		return (dataNodeCount - 1);
	}
	
	public 
	
	
	
	
	public static void main(String[] args) {

	}

}
