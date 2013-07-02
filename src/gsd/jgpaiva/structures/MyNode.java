package gsd.jgpaiva.structures;

import peersim.config.Configuration;
import peersim.core.Cleanable;
import peersim.core.CommonState;
import peersim.core.Fallible;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.Protocol;

public class MyNode implements Node, Comparable<MyNode> {
	// ================= fields ========================================
	// =================================================================

	/** used to generate unique IDs */
	private static long counterID = -1;

	/**
	 * The protocols on this node.
	 */
	protected Protocol[] protocol = null;

	/**
	 * The current index of this node in the node list of the {@link Network}.
	 * It can change any time. This is necessary to allow the implementation of
	 * efficient graph algorithms.
	 */
	private int index;

	/**
	 * The fail state of the node.
	 */
	protected byte failstate = Fallible.OK;

	/**
	 * The ID of the node. It should be final, however it can't be final because
	 * clone must be able to set it.
	 */
	private long ID;

	// ================ constructor and initialization =================
	// =================================================================

	/**
	 * Used to construct the prototype node. This class currently does not have
	 * specific configuration parameters and so the parameter
	 * <code>prefix</code> is not used. It reads the protocol components
	 * (components that have type {@value peersim.core.Node#PAR_PROT}) from the
	 * configuration.
	 */
	public MyNode(String prefix) {
		String[] names = Configuration.getNames(Node.PAR_PROT);
		CommonState.setNode(this);
		this.ID = this.nextID();
		this.protocol = new Protocol[names.length];
		for (int i = 0; i < names.length; i++) {
			CommonState.setPid(i);
			Protocol p = (Protocol)
					Configuration.getInstance(names[i]);
			this.protocol[i] = p;
		}
	}

	// -----------------------------------------------------------------

	@Override
	public Object clone() {
		MyNode result = null;
		try {
			result = (MyNode) super.clone();
		} catch (CloneNotSupportedException e) {
		} // never happens
		result.protocol = new Protocol[this.protocol.length];
		CommonState.setNode(result);
		result.ID = this.nextID();
		for (int i = 0; i < this.protocol.length; ++i) {
			CommonState.setPid(i);
			result.protocol[i] = (Protocol) this.protocol[i].clone();
		}
		return result;
	}

	// -----------------------------------------------------------------

	/** returns the next unique ID */
	private long nextID() {
		return MyNode.counterID++;
	}

	// =============== public methods ==================================
	// =================================================================

	@Override
	public void setFailState(int failState) {
		// after a node is dead, all operations on it are errors by definition
		if (this.failstate == Fallible.DEAD && failState != Fallible.DEAD)
			throw new IllegalStateException(
					"Cannot change fail state: node is already DEAD");
		switch (failState)
		{
		case OK:
			this.failstate = Fallible.OK;
			break;
		case DEAD:
			// protocol = null;
			this.index = -1;
			this.failstate = Fallible.DEAD;
			for (int i = 0; i < this.protocol.length; ++i)
				if (this.protocol[i] instanceof Cleanable) {
					((Cleanable) this.protocol[i]).onKill();
				}
			break;
		case DOWN:
			this.failstate = Fallible.DOWN;
			break;
		default:
			throw new IllegalArgumentException(
					"failState=" + failState);
		}
	}

	// -----------------------------------------------------------------

	@Override
	public int getFailState() {
		return this.failstate;
	}

	// ------------------------------------------------------------------

	@Override
	public boolean isUp() {
		return this.failstate == Fallible.OK;
	}

	// -----------------------------------------------------------------

	@Override
	public Protocol getProtocol(int i) {
		return this.protocol[i];
	}

	// ------------------------------------------------------------------

	@Override
	public int protocolSize() {
		return this.protocol.length;
	}

	// ------------------------------------------------------------------

	@Override
	public int getIndex() {
		return this.index;
	}

	// ------------------------------------------------------------------

	@Override
	public void setIndex(int index) {
		this.index = index;
	}

	// ------------------------------------------------------------------

	/**
	 * Returns the ID of this node. The IDs are generated using a counter (i.e.
	 * they are not random).
	 */
	@Override
	public long getID() {
		return this.ID;
	}

	/** Implemented as <code>(int)getID()</code>. */
	@Override
	public int hashCode() {
		return (int) this.getID();
	}

	@Override
	public String toString() {
		return "n:" + this.getID();
	}

	@Override
	public boolean equals(Object o) {
		return o instanceof MyNode && this.getID() == ((MyNode)o).getID();
	}
	
	public boolean equals(MyNode o) {
		return this.getID() == o.getID();
	}

	@Override
	public int compareTo(MyNode node) {
		return (int) (this.getID() - node.getID());
	}

	/**
	 * Substitutes the specified protocol at this node.
	 * 
	 * @param pid
	 *            protocol identifier
	 * @param prot
	 *            the protocol object
	 */
	public void setProtocol(int pid, Protocol prot)
	{
		this.protocol[pid] = prot;
	}
}
