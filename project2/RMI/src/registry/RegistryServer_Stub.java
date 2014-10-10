/*
 * Author: Siyuan Zhou, Zichang Feng
 * 
 * The RegistryServer_stub implements the server side functionalities
 * of Registry interface. It provides functionality of binding, rebinding
 * and unbinding an object.
 * 
 * The RegistryServer_Stub is actually a RemoteReference to remote object.
 * All the functions calls are remote method invocations.
 */

package registry;

import messages.RMIClientMessage;
import messages.RMIServerMessage;
import remote.Remote;
import remote.RemoteException;
import remote.RemoteReference;

public class RegistryServer_Stub extends RemoteReference implements Registry {

	//Constructor simply calls RemoteReference constructor to provide
	//remote host information and its remote object name.
	public RegistryServer_Stub(String interfaceName, String host, int port) {
		super(interfaceName, host, port);
	}
	
	/*
	 * All methods are simply remote method invocations.
	 * For interface, please refer to Registry interface.
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

	@Override
	public void bind(String name, Remote obj) throws RemoteException {
		RMIClientMessage msg = 
				new RMIClientMessage(REGISTRY_OBJECT_NAME, "bind", new Object[]{name, obj});
		RMIServerMessage returnValue = invokeRemoteMethod(msg);
		Exception e = returnValue.getException();
		if (e != null)
			throw new RemoteException("remote method invocation fails", e);

	}

	@Override
	public void unbind(String name) throws RemoteException {
		RMIClientMessage msg = 
				new RMIClientMessage(REGISTRY_OBJECT_NAME, "unbind", new Object[]{name});
		RMIServerMessage returnValue = invokeRemoteMethod(msg);
		Exception e = returnValue.getException();
		if (e != null)
			throw new RemoteException("remote method invocation fails", e);
	}

	@Override
	public void rebind(String name, Remote obj) throws RemoteException {
		RMIClientMessage msg = 
				new RMIClientMessage(REGISTRY_OBJECT_NAME, "rebind", new Object[]{name, obj});
		RMIServerMessage returnValue = invokeRemoteMethod(msg);
		Exception e = returnValue.getException();
		if (e != null)
			throw new RemoteException("remote method invocation fails", e);
	}

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
