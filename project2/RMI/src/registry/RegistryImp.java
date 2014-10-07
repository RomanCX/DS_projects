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

	private HashMap<String, ObjectStubPair> objectTable;
	private String hostname;
	
	public RegistryImp() {
		objectTable = new HashMap<String, ObjectStubPair>();
		try {
			hostname = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
	}
	private class ObjectStubPair {
		public Remote object;
		public RemoteReference stub;
		
		public ObjectStubPair(Remote object, RemoteReference stub) {
			this.object = object;
			this.stub = stub;
		}
	}
	
	@Override
	public Remote lookup(String name) throws RemoteException {
		RemoteReference stub = objectTable.get(name).stub;
		if (stub == null) {
			throw new RemoteException("Object with name " + name 
					+ " not found");
		}
		return stub;
	}

	@Override
	public void bind(String name, Remote obj) throws RemoteException {
		if (objectTable.get(name) != null) {
			throw new RemoteException("Object with name " + name +
					" already exist");
		}
		this.rebind(name, obj);
	}

	@Override
	public void unbind(String name) throws RemoteException {
		if (objectTable.get(name) == null) {
			throw new RemoteException("Object with name " + name 
					+ " not found");
		}
		objectTable.remove(name);

	}

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

	@Override
	public void rebind(String name, Remote obj) throws RemoteException {
		RemoteReference stub = StubFactory.createStub(obj.getClass().getName(), 
				name, this.hostname, this.REGISTRY_PORT);
		objectTable.put(name, new ObjectStubPair(obj, stub));
	}
	
	private void checkName(String name) throws RemoteException {
		if (name.equals(REGISTRY_OBJECT_NAME)) {
			throw new RemoteException("Cannot operate on registry name");
		}
	}
	
	public void bindSelf() {
		/*
		 * Registry itself doesn't need a stub stored in the table
		 * because none will call lookup(registryname)
		 */
		objectTable.put(REGISTRY_OBJECT_NAME, new ObjectStubPair(this, null));
	}
	
	public Remote getObject(String name) {
		ObjectStubPair pair = objectTable.get(name);
		if (pair == null) {
			return null;
		}
		return pair.object;
	}

}
