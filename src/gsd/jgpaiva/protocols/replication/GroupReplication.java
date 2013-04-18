package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.GroupSizeObservable;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.structures.dht.FingerGroup;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.Identifier;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.Utils;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.Fallible;
import peersim.core.Node;
import peersim.core.Protocol;

public class GroupReplication extends ProtocolStub implements Protocol, UptimeSimulatorProtocol,
		KeyStorageProtocol, GroupSizeObservable, MonitorableProtocol {
	public static final String PAR_MIN_REPL = "minrepl";
	public static final String PAR_MAX_REPL = "maxrepl";

	private static int idLength;
	static BigInteger ringSize;
	private static KeyCreator keyCreator = KeyCreator.getInstance();
	private static int replication;

	static boolean simStarted = false;

	private static int maxReplication;

	private static int minReplication;

	private SimpleGroup myGroup;
	protected int deathTime;

	// private FingerGroup finger;
	// private FingerGroup successor;
	// private FingerGroup predecessor;
	// private int keys = 0;

	public GroupReplication(String prefix) {
		super(prefix);
		GroupReplication.idLength = GlobalConfig.getIdLength();
		GroupReplication.ringSize = BigInteger.ONE.add(BigInteger.ONE).pow(
				GroupReplication.idLength);
		GroupReplication.minReplication = Configuration.getInt(prefix + '.'
				+ GroupReplication.PAR_MIN_REPL);
		GroupReplication.maxReplication = Configuration.getInt(prefix + '.'
				+ GroupReplication.PAR_MAX_REPL);
	}

	@Override
	public GroupReplication clone() {
		GroupReplication c = (GroupReplication) super.clone();
		c = (GroupReplication) super.clone();
		return c;
	}

	@Override
	public boolean shouldPrint() {
		return true;
	}

	static GroupReplication getProtocol(Node node) {
		GroupReplication val = (GroupReplication) node.getProtocol(ProtocolStub.pid);
		return val;
	}

	@Override
	public void join(Node contact) {
		// step 1: get successor and set as predecessor
		if (SimpleGroup.groups.size() == 0) {
			SimpleGroup seedGroup = SimpleGroup.createSeedGroup(this.getNode());
			seedGroup.updateMembers();
		} else {
			SimpleGroup toJoin = this.getGroupToJoin();
			toJoin.joinNode(this.getNode());
			toJoin.updateMembers();

			if (this.myGroup.size() > GroupReplication.maxReplication) {
				this.myGroup.divide();
			}
		}
		assert (this.myGroup.keys() > 0) : this.getNode() + " " + this.myGroup;
	}

	private SimpleGroup getGroupToJoin() {
		double maxLoad = 0;
		SimpleGroup toReturn = null;

		for (SimpleGroup it : SimpleGroup.groups) {
			double load = ((double) it.keys()) / it.size();
			if (load > maxLoad) {
				maxLoad = load;
				toReturn = it;
			}
		}
		assert (toReturn != null) : Group.groups;
		return toReturn;
	}

	public void killed(Collection<Pair<Node, Integer>> availabilityList) {
		SimpleGroup tempGroup = this.myGroup;
		this.myGroup.removeNode(this.getNode());
		if (tempGroup.size() < GroupReplication.minReplication) {
			tempGroup.merge();
		}
		if (tempGroup.size() > GroupReplication.maxReplication) {
			tempGroup.divide();
		}
	}

	@Override
	public void startup(Node myNode, Collection<Pair<Node, Integer>> availabilityList,
			int deathTime, int currentTime) {
		GroupReplication.checkIntegrity();
		assert (myNode.getFailState() == Fallible.DOWN);
		myNode.setFailState(Fallible.OK);
		this.deathTime = deathTime;
		this.triggerJoin(myNode, myNode);
		GroupReplication.checkIntegrity();
	}

	@Override
	public void kill(Collection<Pair<Node, Integer>> availabilityList) {
		GroupReplication.checkIntegrity();
		assert (this.getNode().getFailState() == Fallible.OK);
		this.killed(availabilityList);
		this.getNode().setFailState(Fallible.DOWN);
		GroupReplication.checkIntegrity();
	}

	@Override
	public boolean isUp() {
		return this.getNode() == null ? false : this.getNode().isUp();
	}

	static final class KeyTransferMessage implements CostAwareMessage {
		private int size;

		public KeyTransferMessage(int size) {
			this.size = size;
		}

		@Override
		public Long getCost() {
			return (long) this.size;
		}
	}

	@Override
	public void startSim(Node myNode) {
		GroupReplication.simStarted = true;
	}

	@Override
	public String toString() {
		return this.myGroup + " " + this.isUp();
	}

	public void setGroup(SimpleGroup group) {
		this.myGroup = group;
	}

	public void sendMessage(KeyTransferMessage keyTransferMessage) {
		if (GroupReplication.simStarted) {
			this.addCost(keyTransferMessage);
		}
	}

	@Override
	public int getKeys() {
		return this.myGroup != null ? this.myGroup.keys() : 0;
	}

	@Override
	public int getGroupSize() {
		return this.myGroup != null ? this.myGroup.size() : 0;
	}

	@Override
	public int getMaxGroupSize() {
		return GroupReplication.maxReplication;
	}

	@Override
	public int getMinGroupSize() {
		return GroupReplication.minReplication;
	}

	public static int getRoundMerges() {
		return SimpleGroup.getMergesCount();
	}

	public static int getRoundDivisions() {
		return Group.getDivisionsCount();
	}

	public static int getRoundJoins() {
		return Group.getJoinCount();
	}

	public static int getRoundLeaves() {
		return Group.getLeaveCount();
	}

	@Override
	public int getReplicationDegree() {
		return this.getGroupSize();
	}

	public static HashSet<TreeSet<Node>> getGroups() {
		HashSet<TreeSet<Node>> toRet = new HashSet<TreeSet<Node>>();
		for (Group it : Group.groups) {
			toRet.add(it.getFinger());
		}
		return toRet;
	}

	static boolean checkIntegrity() {
		int keysSum = 0;
		for (Group it : Group.groups) {
			keysSum += it.keys();
		}
		assert (Group.groups.size() == 0 || keysSum == GroupReplication.keyCreator.getNKeys()) : keysSum
				+ " " + GroupReplication.keyCreator.getNKeys();
		return true;
	}

	@Override
	public int getMonitoringCount() {
		return this.getGroupSize() - 1;
	}
}

class SimpleGroup {
	static final HashSet<SimpleGroup> groups = new HashSet<SimpleGroup>();
	private static int joinCount = 0;
	private static int leaveCount = 0;
	private static int mergesCount = 0;
	private static int divisionsCount = 0;

	private TreeSet<Node> finger;
	private SimpleGroup successor;
	private SimpleGroup predecessor;
	private int keys = 0;

	TreeSet<Node> getFinger() {
		return this.finger;
	}

	private SimpleGroup() {
	}

	public static int getMergesCount() {
		int toReturn = SimpleGroup.mergesCount;
		SimpleGroup.mergesCount = 0;
		return toReturn;
	}

	public static int getDivisionsCount() {
		int toReturn = SimpleGroup.divisionsCount;
		SimpleGroup.divisionsCount = 0;
		return toReturn;
	}

	public static int getJoinCount() {
		int toReturn = SimpleGroup.joinCount;
		SimpleGroup.joinCount = 0;
		return toReturn;
	}

	public static int getLeaveCount() {
		int toReturn = SimpleGroup.leaveCount;
		SimpleGroup.leaveCount = 0;
		return toReturn;
	}

	public int keys() {
		return this.keys;
	}

	void merge() {
		GroupReplication.checkIntegrity();
		if (SimpleGroup.groups.size() == 1) return;
		assert (this.successor != null);
		SimpleGroup.mergesCount++;

		boolean ret = SimpleGroup.groups.remove(this);
		assert (ret);
		int oldKeys = this.keys;
		int successorKeys = this.successor.keys;
		this.successor.finger.addAll(this.finger);
		this.successor.keys = successorKeys + oldKeys;
		assert (this.keys >= 0);
		assert (this.successor.keys >= 0) : this.successor.keys + " " + this.keys;
		if (this.predecessor == this.successor) {
			this.successor.predecessor = null;
			this.successor.successor = null;
		} else {
			this.predecessor.successor = this.successor;
			this.successor.predecessor = this.predecessor;
		}
		for (Node it : this.successor.finger) {
			GroupReplication.getProtocol(it).sendMessage(
					new GroupReplication.KeyTransferMessage(oldKeys));
		}
		for (Node it : this.finger) {
			GroupReplication.getProtocol(it).sendMessage(
					new GroupReplication.KeyTransferMessage(successorKeys));
		}
		this.successor.updateMembers();
		GroupReplication.checkIntegrity();
	}

	void divide() {
		SimpleGroup.divisionsCount++;
		final int initialSize = this.finger.size();
		TreeSet<Node> setNew = new TreeSet<Node>();
		TreeSet<Node> oldGroup = new TreeSet<Node>();
		int newSize = initialSize / 2;
		int oldSize = initialSize - newSize;

		assert (this.keys >= 0);
		assert (oldSize >= newSize) : oldSize + " " + newSize + " " +
				this.finger.size();
		assert (oldSize >= 0) : oldSize + " " + newSize;
		int newKeys = (int) (this.keys / (((double) initialSize) / ((double)
				newSize)));
		int oldKeys = this.keys - newKeys;
		assert (newKeys > 0) : newKeys + " " + initialSize + " " + newSize +
				" " + this.keys;
		assert (oldKeys > 0) : oldKeys;

		while (this.finger.size() > oldSize) {
			setNew.add(Utils.removeRandomEl(this.finger));
		}

		// TreeSet<Node> candidates = Group.getCandidates(this.finger);
		// int candidatesSize = candidates.size();
		// for (int it = 0; it < candidatesSize; it++) {
		// Node fst = candidates.pollFirst();
		// if (it % 2 == 0) {
		// boolean ret = oldGroup.add(fst);
		// assert (ret);
		// } else {
		// boolean ret = setNew.add(fst);
		// assert (ret);
		// }
		// }
		// assert (candidates.isEmpty());
		// this.finger = oldGroup;

		SimpleGroup newGroup = this.createGroup(setNew);

		assert (newGroup.size() + this.size() == initialSize) : newGroup.size() + " " + this.size()
				+ " " + initialSize + " " + oldGroup.size() + " " + setNew.size();

		if (this.predecessor != null) {
			newGroup.predecessor = this.predecessor;
			this.predecessor.successor = newGroup;
			this.predecessor = newGroup;
			newGroup.successor = this;
		} else {
			this.predecessor = newGroup;
			this.successor = newGroup;
			newGroup.predecessor = this;
			newGroup.successor = this;
		}
		newGroup.updateMembers();
		this.updateMembers();
		newGroup.keys = newKeys;
		this.keys = oldKeys;
	}

	private static TreeSet<Node> getCandidates(TreeSet<Node> set) {
		// candidates sorted by descending death time
		TreeSet<Node> candidates = new TreeSet<Node>(
				new Comparator<Node>() {
					@Override
					public int compare(Node o1, Node o2) {
						GroupReplication g1 = (GroupReplication) o1.getProtocol(ProtocolStub
								.getPID());
						GroupReplication g2 = (GroupReplication) o2.getProtocol(ProtocolStub
								.getPID());
						int diff = g1.deathTime - g2.deathTime;
						if (diff == 0) {
							diff = g1.getNode().getIndex() - g2.getNode().getIndex();
						}
						return -diff;
					};
				});

		for (Node it : set) {
			candidates.add(it);
		}
		assert (set.size() == candidates.size());
		return candidates;
	}

	private SimpleGroup createGroup(TreeSet<Node> setNew) {
		SimpleGroup toReturn = new SimpleGroup();
		toReturn.finger = setNew;
		SimpleGroup.groups.add(toReturn);
		return toReturn;
	}

	public int size() {
		return this.finger.size();
	}

	void joinNode(Node node) {
		SimpleGroup.joinCount++;
		this.finger.add(node);
		GroupReplication.getProtocol(node).sendMessage(
				new GroupReplication.KeyTransferMessage(this.keys));
	}

	void removeNode(Node node) {
		SimpleGroup.leaveCount++;
		this.finger.remove(node);
		GroupReplication.getProtocol(node).setGroup(null);
	}

	static SimpleGroup createSeedGroup(Node node) {
		TreeSet<Node> set = new TreeSet<Node>();
		set.add(node);
		SimpleGroup toReturn = new SimpleGroup();
		toReturn.finger = new FingerGroup(set, new Identifier(BigInteger.ZERO));
		toReturn.keys = KeyCreator.getInstance().getNKeys();
		SimpleGroup.groups.add(toReturn);
		return toReturn;
	}

	void updateMembers() {
		for (Node it : this.finger) {
			GroupReplication.getProtocol(it).setGroup(this);
		}
	}

	@Override
	public String toString() {
		return this.keys + "" + this.finger + "";
	}
}
