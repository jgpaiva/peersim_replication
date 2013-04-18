package gsd.jgpaiva.protocols;

import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.Killable;
import gsd.jgpaiva.interfaces.ProtocolInitializableFromNode;
import gsd.jgpaiva.interfaces.SecondaryInitializableProtocol;
import gsd.jgpaiva.interfaces.WeightedMessageCostObservable;
import gsd.jgpaiva.observers.Debug;
import gsd.jgpaiva.utils.IntCounter;
import gsd.jgpaiva.utils.LongCounter;
import gsd.jgpaiva.utils.Pair;

import java.util.HashMap;
import java.util.Map;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;

public abstract class ProtocolStub implements WeightedMessageCostObservable, Cloneable,
		ProtocolInitializableFromNode, SecondaryInitializableProtocol, Killable {
	private static final String PAR_STEP = "step";

	protected static String name;
	protected static int pid;
	protected static long step;

	private Node myNode;

	// Simulation control and observers
	private Map<Class<?>, Pair<IntCounter, LongCounter>> messages;

	public ProtocolStub(String prefix) {
		ProtocolStub.name = prefix;
		ProtocolStub.pid = Configuration.lookupPid(prefix.replace("protocol.", ""));
		ProtocolStub.step = Configuration.getLong(prefix + '.' + ProtocolStub.PAR_STEP);
		this.messages = new HashMap<Class<?>, Pair<IntCounter, LongCounter>>(40);
	}

	@Override
	public ProtocolStub clone() {
		ProtocolStub c = null;
		try {
			c = (ProtocolStub) super.clone();
			c.messages = new HashMap<Class<?>, Pair<IntCounter, LongCounter>>(40);
		} catch (CloneNotSupportedException e) {
			this.DIE();
		}
		return c;
	}

	private static Debug debugInstance = null;

	protected void debug(String s) {
		ProtocolStub.debugInstance.debug(this, s);
	}

	protected boolean debug = false;

	private boolean initialized = false;

	protected void DIE() {
		this.DIE("oops");
	}

	protected void DIE(String string) {
		this.error("CRITICAL: " + string);
		System.exit(-1);
	}

	protected void error(String string) {
		Thread.dumpStack();
		String toPrint = CommonState.getTime() + " " + ProtocolStub.name + " "
				+ this.myNode.getID() + " ERROR " + " " + string;
		System.err.println(toPrint);
		if (this.debug) {
			this.debug(toPrint);
		}
	}

	public abstract boolean shouldPrint();

	protected void print(String string) {
		if (!this.shouldPrint())
			return;
		String toPrint = CommonState.getTime() + " " + ProtocolStub.name + " "
				+ this.myNode.getID() + " INFO " + " " + string;
		System.err.println(toPrint);
		if (this.debug) {
			this.debug(toPrint);
		}
	}

	protected void addCost(CostAwareMessage message) {
		Pair<IntCounter, LongCounter> temp = this.messages.get(message.getClass());
		if (temp != null) {
			temp.fst.value++;
			temp.snd.value += message.getCost();
		} else {
			Pair<IntCounter, LongCounter> newItem = new Pair<IntCounter, LongCounter>(
					new IntCounter(), new LongCounter(message.getCost()));
			this.messages.put(message.getClass(), newItem);
		}
	}

	@Override
	public boolean isInitialized() {
		return this.myNode != null;
	}

	@Override
	public boolean isUp() {
		return this.getNode() == null ? false : this.getNode().isUp();
	}

	protected abstract void join(Node contact);

	@Override
	public void triggerJoin(Node myNode, Node contact) {
		if (!this.initialized) {
			this.initialized = true;
			this.myNode = myNode;
			if (gsd.jgpaiva.observers.Debug.getInstance().contains(myNode)) {
				this.debug = true;
				ProtocolStub.debugInstance = Debug.getInstance();
			}
		}
		this.join(contact);
	}

	@Override
	public void triggerSecondJoin(Node myNode, Node contact) {
		this.triggerJoin(myNode, contact);
	}

	@Override
	public Map<Class<?>, Pair<IntCounter, LongCounter>> getWeightedSentMessages() {
		return this.messages;
	}

	@Override
	public void resetSentMessages() {
		this.messages.clear();
	}

	public Node getNode() {
		return this.myNode;
	}

	public String getName() {
		return ProtocolStub.name;
	}

	@Override
	public boolean isContact() {
		return this.isInitialized();
	}

	public static int getPID() {
		return ProtocolStub.pid;
	}
}
