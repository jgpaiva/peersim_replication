package gsd.jgpaiva.interfaces;

import peersim.core.Node;

public interface ProtocolInitializableFromNode {
	public void triggerJoin(Node myNode, Node contact);

	public boolean isContact();
}
