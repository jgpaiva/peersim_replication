package gsd.jgpaiva.interfaces;

import peersim.core.Node;

public interface SecondaryInitializableProtocol {
	public void triggerSecondJoin(Node myNode, Node contact);
}
