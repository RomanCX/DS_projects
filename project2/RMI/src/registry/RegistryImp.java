/*
 * Author: Siyuan Zhou, Zichang Feng
 * 
 * The RegistryImp class is the real impelmentation of Registry interface 
 * and do all the job as RMIRegistry side. The client and server registry
 * stubs are remotely invoking methods in this class. 
 * 
 * The functionalities are: for client, lookup a remote object reference
 * and get a list of remote objects; for server, bind, rebind and unbind
 * an object with name.
.
 */

package registry;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import remote.Remote;
import remote.RemoteException;
import remote.RemoteReference;

public class RegistryImp implements Registry {

	//Table to store mapping between names and objects
	private HashMap<String, ObjectStubPair> objectTable;
	//Host name of RMIRegistry(typically localhost)
	//Useful when creating stub instance from object instance
	private String hostname;
	//Constructor initialize class fields
	public RegistryImp() {
		objectTable = new HashMap<String, ObjectStubPair>();
		try {
			hostname = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	
	//A object-stub pair structure. Used as the value in the table.
	private class ObjectStubPair {
		public Remote object;
		public RemoteReference stub;
		
		public ObjectStubPair(Remote object, RemoteReference stub) {
			this.object = object;
			this.stub = stub;
		}
	}
	/*
	 * Lookup a remote object with a name. If found, return 
	 * the RemoteReference(the stub)
	 * Throw RemoteException if name not found
	 */
	@Override
	public Remote lookup(String name) throws RemoteException {
		checkName(name);
		RemoteReference stub = objectTable.get(name).stub;
		if (stub == null) {
			throw new RemoteException("Object with name " + name 
					+ " not found");
		}
		return stub;
	}

    /*
     * Bind the object with a name.
     * Throw RemoteException if name already exists
     * or if the function is called by a client.
     */
	@Override
	public void bind(String name, Remote obj) throws RemoteException {
		if (objectTable.get(name) != null) {
			throw new RemoteException("Object with name " + name +
					" already exist");
		}
		this.rebind(name, obj);
		System.out.println("Binding object " + obj.toString() + " with name " + name);
	}

    /*
     * Lookup the object with name and delete it.
     * Throw RemoeteException if the name doesn't exist
     * or if the function is called by a client
     */
	@Override
	public void unbind(String name) throws RemoteException {
		checkName(name);
		if (objectTable.get(name) == null) {
			throw new RemoteException("Object with name " + name 
					+ " not found");
		}
		objectTable.remove(name);

	}

	/*
	 * Returns a list of names already binded.
	 */
	@Override
	public String[] list() throws RemoteException {
		String[] result = new String[objectTable.size()];
		Iterator it = objectTable.entrySet().iterator();
		for (int i = 0; i < objectTable.size(); i++) {
			Map.Entry pairs = (Map.Entry) it.next();
			result[i] = (String) pairs.getKey();
		}
		return result;
	}

    /*
     * Update the table and replace the entry with name and obj.
     * Throw RemoteException if the function is called by a client.
     */
	@Override
	public void rebind(String name, Remote obj) throws RemoteException {
		checkName(name);
		RemoteReference stub = StubFactory.createStub(obj.getClass().getName(), 
				name, this.hostname, this.REGISTRY_PORT);
		objectTable.put(name, new ObjectStubPair(obj, stub));
	}
	
	/*
	 * Check if the object name is the same as registry itself.
	 * If so, throw RemoteException
	 */
	private void checkName(String name) throws RemoteException {
		if (name.equals(REGISTRY_OBJECT_NAME)) {
			throw new RemoteException("Cannot operate on registry name");
		}
	}
	
	/*
	 * Bind registry itself to the table. The registry entry
	 * in the table is used for the convenience fore remote calls
	 * to registry. The entry should not be lookup, bind, rebind or unbind.
	 */
	public void bindSelf() {
		/*
		 * Registry itself doesn't need a stub stored in the table
		 * because none will call lookup(registryname)
		 */
		objectTable.put(REGISTRY_OBJECT_NAME, new ObjectStubPair(this, null));
	}
	
	/*
	 * Get the object(rather than the RemoteReference as lookup).
	 * Called by RMIRegistry to perform method invocation.
	 */
	public Remote getObject(String name) {
		ObjectStubPair pair = objectTable.get(name);
		if (pair == null) {
			return null;
		}
		return pair.object;
	}

}
