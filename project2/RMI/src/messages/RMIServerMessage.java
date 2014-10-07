package messages;

import java.io.Serializable;

public class RMIServerMessage implements Serializable{
	private static final long serialVersionUID = -8270255187769367740L;
	
	private Object returnValue;
	private Exception exception;
	
	//The following two constructors are used when exception occurred
	public RMIServerMessage(Exception e) {
		this.returnValue = null;
		this.exception = e;
	}

	public RMIServerMessage(String exceptionMessage) {
		this.returnValue = null;
		this.exception = new Exception(exceptionMessage);
	}
	
	//This constructor is used when no exception occurred
	public RMIServerMessage(Object returnObject) {
		this.returnValue = returnObject;
		this.exception = null;
	}
	
	public void setReturnValue(Object returnValue) {
		this.returnValue = returnValue;
	}
	
	public void setException(Exception exception) {
		this.exception = exception;
	}
	
	public Object getReturnValue() {
		return returnValue;
	}
	
	public Exception getException() {
		return exception;
	}
}
