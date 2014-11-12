package protocals;

import java.net.InetAddress;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;

import namenode.DatanodeInfo;

public interface ClientProtocal extends Remote{
	/*
	 * Returns hashmap<block id, datanode info>
	 */
	public TreeMap<Integer, DatanodeInfo > read(String fileName, String address) throws RemoteException;
	/*
	 * Returns hashmap<block id, list of datanode info>
	 */
	public TreeMap<Integer, List<DatanodeInfo> >write(String fileName, int splitNum) throws RemoteException;
	
	/*
	 *  Get the size of block
	 */
	public int getBlockSize() throws RemoteException;
	
	public void delete(String fileName) throws RemoteException;
}
