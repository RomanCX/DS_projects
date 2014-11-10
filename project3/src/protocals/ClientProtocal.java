package protocals;

import java.net.InetAddress;
import java.rmi.Remote;
import java.util.HashMap;
import java.util.List;

public interface ClientProtocal extends Remote{
	/*
	 * Returns hashmap<datanodeId, list of Block Ids>
	 */
	public HashMap<Integer, List<Integer> > read(String fileName, String address);
	/*
	 * Returns hashmap<block id, list of datanode id>
	 */
	public HashMap<Integer, List<Integer> >write(String fileName, int splitNum);
}
