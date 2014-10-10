/*
 * Author: Siyuan Zhou, Zichang Feng
 * 
 * The Registry interface provides functionality of a bootstrap point
 * for looking for remote objects.
 * The functionalities are: for client, lookup a remote object reference
 * and get a list of remote objects; for server, bind, rebind and unbind
 * an object with name.
 */

package registry;


import remote.Remote;
import remote.RemoteException;


public interface Registry extends Remote {
	//Port number for registry at RMIRegistry to listen on
	public static final int REGISTRY_PORT = 1099;
	//The name for registry to bind itself
	public static final String REGISTRY_OBJECT_NAME = "registry";
	/*
	 * Lookup a remote object with a name. If found, return 
	 * the RemoteReference(the stub)
	 * Throw RemoteException if name not found
	 */
    public Remote lookup(String name)
    	throws RemoteException;
    /*
     * Bind the object with a name.
     * Throw RemoteException if name already exists
     * or if the function is called by a client.
     */
    public void bind(String name, Remote obj)
    	throws RemoteException;
    /*
     * Lookup the object with name and delete it.
     * Throw RemoeteException if the name doesn't exist
     * or if the function is called by a client
     */
    public void unbind(String name)
    	throws RemoteException;
    /*
     * Update the table and replace the entry with name and obj.
     * Throw RemoteException if the function is called by a client.
     */
	public void rebind(String name, Remote obj)
    	throws RemoteException;
	/*
	 * Returns a list of names already binded.
	 */
    public String[] list() 
    		throws RemoteException;
}
