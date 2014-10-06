package messages;
import java.io.Serializable;



public class RMIClientMessage implements Serializable {
	private static final long serialVersionUID = -3938652368419444573L;
	
	private String objectName;
	private String methodName; 
	private Object[] parameters; 
	
	public RMIClientMessage(String objectName, String methodName, Object[] parameters) {
		this.objectName = objectName;
		this.methodName = methodName;
		this.parameters = parameters;
	}
	
	public String getMethodName() {
		return methodName;
	}
	
	public Object[] getParameters() {
		return parameters;
	}
	
	public String getObjectName() {
		return objectName;
	}
}