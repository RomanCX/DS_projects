/*
 * Author: Siyuan Zhou, Zichang Feng
 * 
 * This class is used to get a reference of registry for 
 * client and server, and is used to create a registry for
 * RMIRegistry class. 
 * For client, the class serves as a bootstrap
 * point to get the first copy of a remote object. 
 * For server, the class can help get a registry for
 * server to bind an object. Note that the class type returned
 * to server is not the same as client. But they share the same
 * interface.
 * The RMIRegistry can call createRegistry to get a real copy of 
 * a class implementing Registry. This reference can do real jobs
 * such as bind and lookup. The references returned by getRegistry are
 * RemoteReferences of the one returned by createRegistry. 
 */

package registry;

import java.net.InetAddress;
import java.net.UnknownHostException;


public class LocateRegistry {
	/*
	 * Disable the default constructor, such that all the functions
	 * are called as static function. It's meaningless to create an
	 * instance of LocateRegistry class.
	 */
	private LocateRegistry() {};
	
	/*
	 * If host is null, return a RegistryServer_Stub with localhost
	 * as host. This is the case when the function is called by server.
	 * 
	 * If host is not null, return a RegistryClient_Stub with the specified
	 * host. THis is the case when he function is called by client.
	 */
	public static Registry getRegistry(String host) throws UnknownHostException {
		if (host == null) {
			return new RegistryServer_Stub(Registry.REGISTRY_OBJECT_NAME, 
					InetAddress.getLocalHost().getHostAddress(), Registry.REGISTRY_PORT);
		} else {
			return new RegistryClient_Stub(Registry.REGISTRY_OBJECT_NAME, 
					host, Registry.REGISTRY_PORT);
		}
	}
	
	/*
	 * THis function is called by server.
	 */
	public static Registry getRegistry()throws UnknownHostException {
		return getRegistry(null);
	}
	
	/*
	 * Creates a real implementation of Registry. This function
	 * should be called by RMIRegistry.
	 */
	public static Registry createRegistry() {
		return new RegistryImp();
	}
}
