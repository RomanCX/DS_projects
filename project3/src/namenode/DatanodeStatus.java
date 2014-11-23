package namenode;

import java.io.Serializable;

public enum DatanodeStatus implements Serializable {
	ALIVE,
	DEAD,
	SHUT_DOWN
}
