/*
 * Author: Siyuan Zhou, Zichang Feng
 * 
 * The RegistryClient_stub implements the client side functionalities
 * of Registry interface. It provides functionality of a bootstrap point
 * for looking for remote objects such as lookup a remote object reference
 * and get a list of remote objects.
 * Performing server side operations such as bind, unbind is forbidden in 
 * this class
 * 
 * The RegistryClient_Stub is actually a RemoteReference to remote object.
 * All the functions calls are remote method invocations.
 */

package registry;

import messages.RMIClientMessage;
import messages.RMIServerMessage;
import remote.Remote;
import remote.RemoteException;
import remote.RemoteReference;

public class RegistryClient_Stub extends RemoteReference implements Registry {

	//Constructor simply calls RemoteReference constructor to provide
	//remote host information and its remote object name.
	public RegistryClient_Stub(String interfaceName, String host, int port) {
		super(interfaceName, host, port);
	}
	
	/*
	 * Lookup a remote object with a name. If found, return 
	 * the RemoteReference(the stub)
	 * Throw RemoteException if name not found
	 */
	@Override
	public Remote lookup(String name) throws RemoteException {
		RMIClientMessage msg = 
				new RMIClientMessage(REGISTRY_OBJECT_NAME, "lookup", new Object[]{name});
		RMIServerMessage returnValue = invokeRemoteMethod(msg);
		Exception e = returnValue.getException();
		if (e != null)
			throw new RemoteException("remote method invocation fails", e);
		return (Remote)returnValue.getReturnValue();
	}
	/*
	 * The following three methods are forbidden in client side.
	 */
	@Override
	public void bind(String name, Remote obj) throws RemoteException {
		throw new RemoteException("Access denied. Can't bind from client side");

	}
	@Override
	public void unbind(String name) throws RemoteException {
		throw new RemoteException("Access denied. Can't unbind from client side");
	}

	@Override
	public void rebind(String name, Remote obj) throws RemoteException {
		throw new RemoteException("Access denied. Can't rebind from client side");

	}

	/*
	 * Returns a list of names already binded.
	 */
	@Override
	public String[] list() throws RemoteException {
		RMIClientMessage msg = 
				new RMIClientMessage(REGISTRY_OBJECT_NAME, "list", new Object[]{});
		RMIServerMessage returnValue = invokeRemoteMethod(msg);
		Exception e = returnValue.getException();
		if (e != null)
			throw new RemoteException("remote method invocation fails", e);
		return (String[])returnValue.getReturnValue();
	}

}
