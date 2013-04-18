package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.interfaces.ChainReplProtocol;
import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.GroupSizeObservable;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.protocols.replication.GroupReplication2.Mode;
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
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Fallible;
import peersim.core.Node;
import peersim.core.Protocol;

public class GroupReplication2 extends ProtocolStub implements Protocol, UptimeSimulatorProtocol,
		KeyStorageProtocol, GroupSizeObservable, MonitorableProtocol, ChainReplProtocol {
	public enum Mode {
		// policies
		SCATTER,
		SCATTER_NOREBALANCE,
		LNLB,
		PREEMPTIVE,
		SUPERSIZE,
		SURPLUS,
		RANDOM,
		SMALLEST_PREEMPTIVE,
		LNLB_MERGE,
		RANDOM2,
		RANDOM3,
		LNLB_SMALLEST,
		LNLB_REBALANCE,
		PREEMPTIVE_GROUP,
		LNLB_SUPERSIZE,
		LNLB_RESCUE,
		LNLB_PREEMPTIVE
	}

	public static final String PAR_MIN_REPL = "minrepl";
	public static final String PAR_MAX_REPL = "maxrepl";
	private static final String MODE = "mode";
	private static final String PAR_UNEVEN_LOAD = "unevenload";

	private static int idLength;
	static BigInteger ringSize;
	private static KeyCreator keyCreator = KeyCreator.getInstance();

	static boolean simStarted = false;

	private static int maxReplication;

	static int minReplication;

	private Group myGroup;
	protected int deathTime;
	private int chainMessageCounter = 0;
	static Mode mode;
	public static Boolean unevenLoad;
	private Joiner joiner;

	// private FingerGroup finger;
	// private FingerGroup successor;
	// private FingerGroup predecessor;
	// private int keys = 0;

	public GroupReplication2(String prefix) {
		super(prefix);
		GroupReplication2.idLength = GlobalConfig.getIdLength();
		GroupReplication2.ringSize = BigInteger.ONE.add(BigInteger.ONE).pow(
				GroupReplication2.idLength);
		GroupReplication2.minReplication = Configuration.getInt(prefix + '.'
				+ GroupReplication2.PAR_MIN_REPL);
		GroupReplication2.maxReplication = Configuration.getInt(prefix + '.'
				+ GroupReplication2.PAR_MAX_REPL);
		GroupReplication2.unevenLoad = Configuration.getBoolean(prefix + '.'
				+ GroupReplication2.PAR_UNEVEN_LOAD);
		String modeString = Configuration.getString(prefix + '.' + GroupReplication2.MODE);

		if (modeString.equals("scatter"))
			mode = Mode.SCATTER;
		else if (modeString.equals("scatter_norebalance"))
			mode = Mode.SCATTER_NOREBALANCE;
		else if (modeString.equals("lnlb"))
			mode = Mode.LNLB;
		else if (modeString.equals("lnlb_supersize")) {
			mode = Mode.LNLB_SUPERSIZE;
			GroupReplication2.maxReplication *= 2;
		} else if (modeString.equals("lnlb_preemptive"))
			mode = Mode.LNLB_PREEMPTIVE;
		else if (modeString.equals("lnlb_rebalance"))
			mode = Mode.LNLB_REBALANCE;
		else if (modeString.equals("lnlb_smallest"))
			mode = Mode.LNLB_SMALLEST;
		else if (modeString.equals("lnlb_merge"))
			mode = Mode.LNLB_MERGE;
		else if (modeString.equals("preemptive"))
			mode = Mode.PREEMPTIVE;
		else if (modeString.equals("rescue"))
			mode = Mode.LNLB_RESCUE;
		else if (modeString.equals("preemptive_group"))
			mode = Mode.PREEMPTIVE_GROUP;
		else if (modeString.equals("supersize")) {
			mode = Mode.SUPERSIZE;
			GroupReplication2.maxReplication *= 2;
		} else if (modeString.equals("surplus"))
			mode = Mode.SURPLUS;
		else if (modeString.equals("random"))
			mode = Mode.RANDOM;
		else if (modeString.equals("random2"))
			mode = Mode.RANDOM2;
		else if (modeString.equals("random3"))
			mode = Mode.RANDOM3;
		else if (modeString.equals("smallest_preemptive"))
			mode = Mode.SMALLEST_PREEMPTIVE;
		else
			throw new RuntimeException("Could not parse mode?");

		System.err.println("Working in " + mode + " mode");

		if (mode == Mode.LNLB || mode == Mode.LNLB_SUPERSIZE || mode == Mode.LNLB_RESCUE
				|| mode == Mode.LNLB_MERGE || mode == Mode.LNLB_REBALANCE)
			joiner = new JoinAverageMostLoaded();
		if (mode == Mode.LNLB_PREEMPTIVE)
			joiner = new JoinPreemptiveAverageMostLoaded();
		else if (mode == Mode.LNLB_SMALLEST)
			joiner = new JoinSmallestAverageMostLoaded();
		else if (mode == Mode.SCATTER || mode == Mode.SCATTER_NOREBALANCE)
			joiner = new JoinScatter();
		else if (mode == Mode.PREEMPTIVE)
			joiner = new JoinPreemtive();
		else if (mode == Mode.PREEMPTIVE_GROUP)
			joiner = new JoinPreemtiveGroup();
		else if (mode == Mode.SUPERSIZE)
			joiner = new JoinSmallest();
		else if (mode == Mode.SURPLUS)
			joiner = new JoinLargest();
		else if (mode == Mode.RANDOM || mode == Mode.RANDOM2 || mode == Mode.RANDOM3)
			joiner = new JoinRandom();
		else if (mode == Mode.SMALLEST_PREEMPTIVE)
			joiner = new JoinSmallestPreemptive();

		if (mode == Mode.LNLB || mode == Mode.RANDOM3 || mode == Mode.LNLB_SMALLEST
				|| mode == Mode.LNLB_REBALANCE || mode == Mode.PREEMPTIVE_GROUP
				|| mode == Mode.LNLB_SUPERSIZE || mode == Mode.LNLB_RESCUE) {
			Group.keyRangeBreaker = new Group.LoadSpliter();
		} else {
			Group.keyRangeBreaker = new Group.RangeSpliter();
		}

		if (mode == Mode.RANDOM) {
			Group.mergeP = new Group.RandomNotThisPicker();
		} else if (mode == Mode.SURPLUS) {
			Group.mergeP = new Group.LargestPicker();
		} else if (mode == Mode.LNLB_MERGE) {
			Group.mergeP = new Group.LoadedPicker();
		} else if (mode == Mode.RANDOM2) {
			Group.mergeP = new Group.RandomNotThisPicker();
		} else {
			Group.mergeP = new Group.SuccessorPicker();
		}

		if (mode == Mode.RANDOM) {
			Group.divideP = new Group.RandomPicker();
		} else {
			Group.divideP = new Group.ThisPicker();
		}
	}

	@Override
	public GroupReplication2 clone() {
		GroupReplication2 c = (GroupReplication2) super.clone();
		c = (GroupReplication2) super.clone();
		return c;
	}

	@Override
	public boolean shouldPrint() {
		return true;
	}

	static GroupReplication2 getProtocol(Node node) {
		GroupReplication2 val = (GroupReplication2) node.getProtocol(ProtocolStub.pid);
		return val;
	}

	@Override
	public void join(Node contact) {
		// step 1: get successor and set as predecessor
		if (Group.groups.size() == 0) {
			Group seedGroup = Group.createSeedGroup(this.getNode());
			seedGroup.updateMembers();
		} else {
			Group toJoin = this.joiner.getGroupToJoin(this);
			assert toJoin != null : Group.groups;
			toJoin.joinNode(this.getNode());
			toJoin.updateMembers();

			if (this.myGroup.size() > GroupReplication2.maxReplication) {
				this.myGroup.divide();
			}
		}
		if (!(this.myGroup.keys() > 0))
			System.err.println("WARNING: joining a node without keys:" + this.getNode() + " " + this.myGroup);
		if (mode == Mode.SCATTER || mode == Mode.LNLB_REBALANCE)
			checkBalance();
	}

	private static void checkBalance() {
		for (int j = 0; j < 100; j++) { // do this a bunch of times between node
										// joins
			int totalMoved = 0;
			for (Group i : Group.groups) {
				int moved = i.checkBalance();
				if (moved > 0) {
					totalMoved++;
				}
			}
			if (totalMoved == 0)
				break;
		}
	}

	interface Joiner {
		Group getGroupToJoin(GroupReplication2 n);
	}

	class JoinAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getMostAverageLoaded(Group.groups);
		}
	}

	class JoinScatter implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getMostLoaded(GRUtils.getSmallestList());
		}
	}

	class JoinSmallestAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getMostAverageLoaded(GRUtils.getSmallestList());
		}
	}

	class JoinPreemptiveAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			double percent = 0.01;
			boolean isAboveAvg = GRUtils.isAboveAverage(n, Group.groups, percent);

			if (isAboveAvg) {
				// List<Pair<Group, Double>> lst3 =
				// GRUtils.getGroupAverageLoadList(Group.groups);
				// Collection<Group> lst4 = GRUtils.slicePercentage(lst3,
				// percent);
				return GRUtils.getMostKeys(Group.groups);
			} else {
				return GRUtils.getMostAverageLoaded(Group.groups);
				// List<Pair<Group, Double>> lst3 =
				// GRUtils.getGroupAverageLoadList(Group.groups);
				// Collection<Group> lst4 = GRUtils.slicePercentage(lst3, 0.5);
				// return GRUtils.getFewestKeys(lst4);
			}
		}
	}

	class JoinPreemtiveGroup implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			List<Pair<Group, Integer>> lst = GRUtils.getGroupDeathList();
			Collection<Group> lst2 = GRUtils.slicePercentage(lst, 0.5);
			return GRUtils.getMostAverageLoaded(lst2);
		}
	}

	class JoinPreemtive implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getShortestDeath(Group.groups);
		}
	}

	class JoinRandom implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return Utils.getRandomEl(Group.groups);
		}
	}

	class JoinSmallestPreemptive implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getShortestDeath(GRUtils.getSmallestList());
		}
	}

	class JoinSmallest implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return Utils.getRandomEl(GRUtils.getSmallestList());
		}
	}

	class JoinLargest implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return Utils.getRandomEl(GRUtils.getLargestList());
		}
	}

	public void killed(Collection<Pair<Node, Integer>> availabilityList) {
		Group tempGroup = this.myGroup;
		this.myGroup.removeNode(this.getNode());

		if (tempGroup.size() <= minReplication && mode == Mode.LNLB_RESCUE)
			rescue(tempGroup);

		if (tempGroup.size() < minReplication) {
			tempGroup.merge();
		}
		if (tempGroup.size() > maxReplication) {
			tempGroup.divide();
		}
	}

	private static void rescue(Group tempGroup) {
		List<Group> lst = GRUtils.getLargestList();
		lst.remove(tempGroup);

		if (lst.size() > 0) {
			Group rgrp = Utils.getRandomEl(lst);
			if (rgrp.size() > minReplication) {
				Node nd = Utils.getRandomEl(rgrp.getFinger());
				rgrp.removeNode(nd);

				tempGroup.joinNode(nd);
			}
		}
	}

	@Override
	public void startup(Node myNode, Collection<Pair<Node, Integer>> availabilityList,
			int myDeathTime, int currentTime) {
		GroupReplication2.checkIntegrity();
		assert (myNode.getFailState() == Fallible.DOWN);
		myNode.setFailState(Fallible.OK);
		this.deathTime = myDeathTime;
		System.out.println(myDeathTime - currentTime);
		this.triggerJoin(myNode, myNode);
		GroupReplication2.checkIntegrity();
	}

	@Override
	public void kill(Collection<Pair<Node, Integer>> availabilityList) {
		GroupReplication2.checkIntegrity();
		assert (this.getNode().getFailState() == Fallible.OK);
		this.killed(availabilityList);
		this.getNode().setFailState(Fallible.DOWN);
		GroupReplication2.checkIntegrity();
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
		GroupReplication2.simStarted = true;
	}

	@Override
	public String toString() {
		return this.myGroup + " " + this.isUp();
	}

	public void setGroup(Group group) {
		this.myGroup = group;
	}

	public void sendMessage(KeyTransferMessage keyTransferMessage) {
		if (GroupReplication2.simStarted) {
			this.addCost(keyTransferMessage);
		}
	}

	@Override
	public int getKeys() {
		return this.myGroup != null ? this.myGroup.keys() : 0;
	}

	public int getLoad() {
		return this.myGroup != null ? this.myGroup.load() : 0;
	}

	@Override
	public int getGroupSize() {
		return this.myGroup != null ? this.myGroup.size() : 0;
	}

	@Override
	public int getMaxGroupSize() {
		return GroupReplication2.maxReplication;
	}

	@Override
	public int getMinGroupSize() {
		return GroupReplication2.minReplication;
	}

	public static int getRoundMerges() {
		return Group.getMergesCount();
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
		assert (Group.groups.size() == 0 || keysSum == GroupReplication2.keyCreator.getNKeys()) : keysSum
				+ " " + GroupReplication2.keyCreator.getNKeys();
		return true;
	}

	@Override
	public int getMonitoringCount() {
		return this.getGroupSize() - 1;
	}

	@Override
	public int getAndClearChainMessages() {
		int retval = this.chainMessageCounter;
		this.chainMessageCounter = 0;
		return retval;
	}
	//
	// @Override
	// public void nextCycle(Node node, int protocolID) {
	// this.chainMessageCounter++;
	// }
}

class Group {
	static final HashSet<Group> groups = new HashSet<Group>();
	// statistics for observers
	private static int joinCount = 0;
	private static int leaveCount = 0;
	private static int mergesCount = 0;
	private static int divisionsCount = 0;
	// behaviour controllers
	static KeyRangeBreaker keyRangeBreaker;
	static GroupPicker mergeP;
	static GroupPicker divideP;
	// group state variables
	private TreeSet<Node> finger;
	Group successor;
	private Group predecessor;
	int keys = 0;
	int load = 0;

	public int checkBalance() {
		if (this.successor == null)
			return 0;

		if (this.successor.keys > 1.6 * this.keys) {
			int totalKeys = successor.keys + this.keys;
			int moved = totalKeys / 2 - this.keys;
			this.keys += moved;
			successor.keys = totalKeys - this.keys;

			for (Node it : this.finger) {
				GroupReplication2.getProtocol(it).sendMessage(
						new GroupReplication2.KeyTransferMessage(moved));
			}
			return moved;
		} else if (1.6 * this.successor.keys < this.keys) {
			int totalKeys = successor.keys + this.keys;
			int moved = totalKeys / 2 - this.successor.keys;
			this.successor.keys += moved;
			this.keys = totalKeys - this.successor.keys;

			for (Node it : this.successor.finger) {
				GroupReplication2.getProtocol(it).sendMessage(
						new GroupReplication2.KeyTransferMessage(moved));
			}
			return moved;
		}
		return 0;
	}

	private Group() {
	}

	private static Group createGroup(TreeSet<Node> setNew) {
		Group toReturn = new Group();
		toReturn.finger = setNew;
		Group.groups.add(toReturn);
		return toReturn;
	}

	static Group createSeedGroup(Node node) {
		TreeSet<Node> set = new TreeSet<Node>();
		set.add(node);
		Group toReturn = new Group();
		toReturn.finger = new FingerGroup(set, new Identifier(BigInteger.ZERO));
		toReturn.keys = KeyCreator.getInstance().getNKeys();
		toReturn.load = KeyCreator.getInstance().getNKeys();
		Group.groups.add(toReturn);
		return toReturn;
	}

	public void merge() {
		mergeTo(mergeP.getGroup(this));
	}

	public void divide() {
		divideTo(divideP.getGroup(this));
	}

	private void mergeTo(Group mergeTo) {
		GroupReplication2.checkIntegrity();
		if (Group.groups.size() == 1)
			return;
		assert (this.successor != null);
		Group.mergesCount++;

		mergeKeysTo(mergeTo);
		mergeNodesTo(mergeTo);

		GroupReplication2.checkIntegrity();
	}

	private void mergeKeysTo(Group mergeTo) {
		int oldKeys = this.keys;
		int mergeToKeys = mergeTo.keys;
		mergeTo.keys = mergeToKeys + oldKeys;
		mergeTo.load = this.load + mergeTo.load;
		assert (oldKeys >= 0 && mergeToKeys >= 0) : oldKeys + " " + mergeToKeys;
		for (Node it : this.successor.finger) {
			GroupReplication2.getProtocol(it).sendMessage(
					new GroupReplication2.KeyTransferMessage(oldKeys));
		}
		for (Node it : this.finger) {
			GroupReplication2.getProtocol(it).sendMessage(
					new GroupReplication2.KeyTransferMessage(mergeToKeys));
		}
	}

	private void mergeNodesTo(Group mergeTo) {
		boolean ret = Group.groups.remove(this);
		assert (ret);
		mergeTo.finger.addAll(this.finger);
		mergeTo.updateMembers();

		if (this.predecessor == this.successor) {
			this.successor.predecessor = null;
			this.successor.successor = null;
		} else {
			this.predecessor.successor = this.successor;
			this.successor.predecessor = this.predecessor;
		}
	}

	private void divideTo(Group divideTo) {
		Group.divisionsCount++;
		if (divideTo == null)
			divideTo = this;

		Group newGroup = splitIntoNewGroup();
		divideTo.moveKeysTo(newGroup);
		divideTo.setAsPredecessor(newGroup);
	}

	private Group splitIntoNewGroup() {
		Group newGroup = null;
		TreeSet<Node> setNew = new TreeSet<Node>();
		int newSize = this.finger.size() / 2;
		int oldSize = this.finger.size() - newSize;

		assert (oldSize >= newSize) : oldSize + " " + newSize + " " +
				this.finger.size();
		assert (oldSize >= 0) : oldSize + " " + newSize;

		if (GroupReplication2.mode == Mode.LNLB_PREEMPTIVE ) {//|| GroupReplication2.mode == Mode.LNLB) {
			while (this.finger.size() > oldSize) {
				Node n = GRUtils.getMinDeathTime(this.finger);	
				this.finger.remove(n);
				setNew.add(n);
			}
		} else {
			while (this.finger.size() > oldSize) {
				setNew.add(Utils.removeRandomEl(this.finger));
			}
		}

		newGroup = Group.createGroup(setNew);

		newGroup.updateMembers();
		this.updateMembers();
		return newGroup;
	}

	private void moveKeysTo(Group newGroup) {
		final int totalSize = newGroup.size() + this.size();
		final int newSize = newGroup.size();

		Pair<Integer, Integer> result = Group.keyRangeBreaker.getNewKeys(totalSize, newSize, this);

		int newKeys = result.fst;
		int newLoad = result.snd;
		int oldKeys = this.keys - newKeys;
		int oldLoad = this.load - newLoad;
		newGroup.keys = newKeys;
		this.keys = oldKeys;
		newGroup.load = newLoad;
		this.load = oldLoad;

		if (!(newKeys > 0 && oldKeys > 0)) {
			System.err.println("WARNING: group with zero keys: newKeys:" + newKeys + " oldKeys:" + oldKeys
					+ " initialSize:" + totalSize + " newSize:" + newSize);
		}
		if (!(newLoad > 0 && oldLoad > 0)) {
			System.err.println("WARNING: group with zero load: newLoad:" + newLoad + " oldLoad:" + oldLoad
					+ " initialSize:" + totalSize + " newLoad:" + newLoad);
		}
	}

	private void setAsPredecessor(Group newGroup) {
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
	}

	public interface KeyRangeBreaker {
		Pair<Integer, Integer> getNewKeys(final int initialSize, final int newSize, Group group);
	}

	public static class RangeSpliter implements KeyRangeBreaker {
		@Override
		public Pair<Integer, Integer> getNewKeys(final int initialSize, final int newSize, Group group) {
			int newKeys = 0;
			for (int i = 0; i < group.keys; i++)
				if (CommonState.r.nextInt(2) == 0)
					newKeys++;
			int newLoad = GroupReplication2.unevenLoad ? group.load / 100 : newKeys;
			return new Pair<Integer, Integer>(newKeys, newLoad);
		}
	}

	public static class LoadSpliter implements KeyRangeBreaker {
		@Override
		public Pair<Integer, Integer> getNewKeys(final int initialSize, final int newSize, Group group) {
			int newLoad = (int) (group.load / (((double) initialSize) / ((double) newSize)));
			int newKeys = (int) (GroupReplication2.unevenLoad ? Math.ceil(group.keys) * 0.10 : group.load);
			return new Pair<Integer, Integer>(newKeys, newLoad);
		}
	}

	public interface GroupPicker {
		public Group getGroup(Group g);
	}

	static class SuccessorPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			return g.successor;
		}
	}

	static class LoadedPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			if (Group.groups.size() == 1)
				return null;
			Group mostLoaded = null;
			for (Group i : Group.groups) {
				if (i != g) {
					if (mostLoaded == null || GRUtils.calcAvgLoad(i) > GRUtils.calcAvgLoad(mostLoaded))
						mostLoaded = i;
				}
			}
			return mostLoaded;
		}
	}

	static class LargestPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			if (Group.groups.size() == 1)
				return null;
			Group largest = null;
			for (Group i : Group.groups) {
				if (i != g) {
					if (largest == null || i.size() > largest.size())
						largest = i;
				}
			}
			return largest;
		}
	}

	static class ThisPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			return g;
		}
	}

	static class RandomPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			return Utils.getRandomEl(Group.groups);
		}
	}

	static class RandomNotThisPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			if (Group.groups.size() == 1)
				return null;
			Group toMerge;
			do {
				toMerge = Utils.getRandomEl(Group.groups);
			} while (toMerge == g);
			return toMerge;
		}
	}

	private static TreeSet<Node> getCandidates(TreeSet<Node> set) {
		// candidates sorted by descending death time
		TreeSet<Node> candidates = new TreeSet<Node>(
				new Comparator<Node>() {
					@Override
					public int compare(Node o1, Node o2) {
						GroupReplication2 g1 = (GroupReplication2) o1.getProtocol(ProtocolStub
								.getPID());
						GroupReplication2 g2 = (GroupReplication2) o2.getProtocol(ProtocolStub
								.getPID());
						int diff = g1.deathTime - g2.deathTime;
						if (diff == 0) {
							diff = g1.getNode().getIndex() - g2.getNode().getIndex();
						}
						return -diff;
					}
				});

		for (Node it : set) {
			candidates.add(it);
		}
		assert (set.size() == candidates.size());
		return candidates;
	}

	void updateMembers() {
		for (Node it : this.finger) {
			GroupReplication2.getProtocol(it).setGroup(this);
		}
	}

	void joinNode(Node node) {
		Group.joinCount++;
		this.finger.add(node);
		GroupReplication2.getProtocol(node).sendMessage(
				new GroupReplication2.KeyTransferMessage(this.keys));
	}

	void removeNode(Node node) {
		Group.leaveCount++;
		this.finger.remove(node);
		GroupReplication2.getProtocol(node).setGroup(null);
	}

	public static int getMergesCount() {
		int toReturn = Group.mergesCount;
		Group.mergesCount = 0;
		return toReturn;
	}

	public static int getDivisionsCount() {
		int toReturn = Group.divisionsCount;
		Group.divisionsCount = 0;
		return toReturn;
	}

	public static int getJoinCount() {
		int toReturn = Group.joinCount;
		Group.joinCount = 0;
		return toReturn;
	}

	public static int getLeaveCount() {
		int toReturn = Group.leaveCount;
		Group.leaveCount = 0;
		return toReturn;
	}

	public int keys() {
		return this.keys;
	}

	public int size() {
		return this.finger.size();
	}

	int load() {
		return this.load;
	}

	TreeSet<Node> getFinger() {
		return this.finger;
	}

	@Override
	public String toString() {
		return this.keys + "" + this.finger + "";
	}
}
