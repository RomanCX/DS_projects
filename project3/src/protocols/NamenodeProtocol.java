package protocols;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import namenode.DatanodeInfo;


public interface NamenodeProtocol extends Remote {
	public DnRegistration register(String address, int port) throws RemoteException	;
	public List<Command> heartBeat(int nodeId, Map<Integer, String> blocks) throws RemoteException;
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
	
	public List<String> ls() throws RemoteException;
	
	public List<String> ls(String prefix) throws RemoteException;
	
	public TreeMap<Integer, List<DatanodeInfo>> getFileBlocks(String fileName) throws RemoteException;
	
	public void shutDown() throws RemoteException;
}
