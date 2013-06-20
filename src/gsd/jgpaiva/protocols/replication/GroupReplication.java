package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.interfaces.ChainReplProtocol;
import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.GroupSizeObservable;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.LoadAwareProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.observers.Debug;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.protocols.replication.Group.RandomSplit;
import gsd.jgpaiva.protocols.replication.Group.ReliabilitySplit;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.KeyCreator.KeyMode;
import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.Utils;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Fallible;
import peersim.core.Node;
import peersim.core.Protocol;

public class GroupReplication extends ProtocolStub implements Protocol, UptimeSimulatorProtocol,
		KeyStorageProtocol, GroupSizeObservable, MonitorableProtocol, ChainReplProtocol, LoadAwareProtocol {
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
	private static final String PAR_SLICE = "slice";
	private static final String PAR_RELIABLE = "reliable";
	private static final String PAR_KEYS_BEFORE_LOAD = "keysbeforeload";

	private static int idLength;
	static BigInteger ringSize;
	static KeyCreator keyCreator;

	static boolean simStarted = false;

	static int maxReplication;

	static int minReplication;

	private Group myGroup;
	protected int deathTime;
	private int chainMessageCounter = 0;
	private boolean isReliable;

	static Mode mode;
	private static double slice;
	private static double reliable;
	private static Joiner joiner;

	/**
	 * Nodes currently active in the system, sorted by death date. More reliable
	 * nodes are at the end
	 */
	private static final TreeSet<Node> activeNodes = new TreeSet<Node>(new Comparator<Node>() {
		@Override
		public int compare(Node o1, Node o2) {
			GroupReplication p1 = (GroupReplication) o1.getProtocol(pid);
			GroupReplication p2 = (GroupReplication) o2.getProtocol(pid);
			if (p1.deathTime == p2.deathTime)
				return (int) (o1.getID() - o2.getID());
			return p2.deathTime - p1.deathTime;
		}
	});

	public GroupReplication(String prefix) {
		super(prefix);
		GroupReplication.idLength = GlobalConfig.getIdLength();
		GroupReplication.ringSize = BigInteger.ONE.add(BigInteger.ONE).pow(
				GroupReplication.idLength);
		GroupReplication.minReplication = Configuration.getInt(prefix + '.'
				+ GroupReplication.PAR_MIN_REPL);
		GroupReplication.maxReplication = Configuration.getInt(prefix + '.'
				+ GroupReplication.PAR_MAX_REPL);
		if (Configuration.contains(prefix + '.' + GroupReplication.PAR_UNEVEN_LOAD))
			throw new RuntimeException(
					GroupReplication.PAR_UNEVEN_LOAD
							+ " is deprecated. Please delete parameter and change keycreator to read distribution.");
		String modeString = Configuration.getString(prefix + '.' + GroupReplication.MODE);

		keyCreator = KeyCreator.initialize(KeyMode.REGULAR_KEY);

		if (modeString.equals("scatter"))
			mode = Mode.SCATTER;
		else if (modeString.equals("scatter_norebalance"))
			mode = Mode.SCATTER_NOREBALANCE;
		else if (modeString.equals("lnlb"))
			mode = Mode.LNLB;
		else if (modeString.equals("lnlb_supersize")) {
			mode = Mode.LNLB_SUPERSIZE;
			GroupReplication.maxReplication *= 2;
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
			GroupReplication.maxReplication *= 2;
			mode = Mode.LNLB;
		} else if (modeString.equals("surplus"))
			mode = Mode.SURPLUS;
		else if (modeString.equals("random"))
			mode = Mode.RANDOM;
		else if (modeString.equals("smallest_preemptive"))
			mode = Mode.SMALLEST_PREEMPTIVE;
		else
			throw new RuntimeException("Could not parse mode?");

		// LNLB_PREEMPTIVE is only set on gsd.jgpaiva.controllers.ResortNetwork
		if (mode == Mode.LNLB_PREEMPTIVE)
			setMode(Mode.LNLB);
		else
			setMode(mode);
	}

	public void setMode(Mode mode) {
		System.err.println("Working in " + mode + " mode");

		if (mode == Mode.LNLB_PREEMPTIVE) {
			GroupReplication.slice = Configuration.getDouble(name + '.'
					+ GroupReplication.PAR_SLICE);
			GroupReplication.reliable = Configuration.getDouble(name + '.'
					+ GroupReplication.PAR_RELIABLE);
			if (Configuration.contains(name + '.'
					+ GroupReplication.PAR_KEYS_BEFORE_LOAD))
				throw new RuntimeException(PAR_KEYS_BEFORE_LOAD + " is deprecated.");
		}

		if (mode == Mode.LNLB || mode == Mode.LNLB_SUPERSIZE || mode == Mode.LNLB_RESCUE
				|| mode == Mode.LNLB_MERGE || mode == Mode.LNLB_REBALANCE)
			GroupReplication.joiner = new JoinAverageMostLoaded();
		if (mode == Mode.LNLB_PREEMPTIVE)
			GroupReplication.joiner = new JoinPreemptiveAverageMostLoaded();
		else if (mode == Mode.LNLB_SMALLEST)
			GroupReplication.joiner = new JoinSmallestAverageMostLoaded();
		else if (mode == Mode.SCATTER || mode == Mode.SCATTER_NOREBALANCE)
			GroupReplication.joiner = new JoinScatter();
		else if (mode == Mode.PREEMPTIVE)
			GroupReplication.joiner = new JoinPreemtive();
		else if (mode == Mode.PREEMPTIVE_GROUP)
			GroupReplication.joiner = new JoinPreemtiveGroup();
		// else if (mode == Mode.SUPERSIZE)
		// GroupReplication.joiner = new JoinSmallest();
		else if (mode == Mode.SURPLUS)
			GroupReplication.joiner = new JoinLargest();
		else if (mode == Mode.RANDOM)
			GroupReplication.joiner = new JoinRandom();
		else if (mode == Mode.SMALLEST_PREEMPTIVE)
			GroupReplication.joiner = new JoinSmallestPreemptive();

		if (mode == Mode.LNLB_PREEMPTIVE) {
			Group.keyRangeBreaker = Group.LoadAndResortSpliter.instance;
		} else {
			Group.keyRangeBreaker = Group.LoadSpliter.instance;
		}

		if (mode == Mode.RANDOM) {
			Group.mergeP = Group.RandomNotThisPicker.instance;
		} else if (mode == Mode.SURPLUS) {
			Group.mergeP = Group.LargestPicker.instance;
		} else if (mode == Mode.LNLB_MERGE) {
			Group.mergeP = Group.LoadedPicker.instance;
		} else {
			Group.mergeP = Group.SuccessorPicker.instance;
		}

		if (mode == Mode.RANDOM) {
			Group.divideP = Group.RandomPicker.instance;
		} else {
			Group.divideP = Group.ThisPicker.instance;
		}

		if (mode == Mode.LNLB_PREEMPTIVE) {
			Group.groupSplitter = ReliabilitySplit.instance;
		} else {
			Group.groupSplitter = RandomSplit.instance;
		}
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

	public static GroupReplication getProtocol(Node node) {
		GroupReplication val = (GroupReplication) node.getProtocol(getPID());
		return val;
	}

	@Override
	public void join(Node contact) {
		activeNodes.add(this.getNode());
		isReliable = GRUtils.isReliable(this.getNode(), activeNodes, reliable);
		// step 1: get successor and set as predecessor
		if (Group.groups.size() == 0) {
			Group seedGroup = Group.createSeedGroup(this.getNode());
			seedGroup.updateMembers();
		} else {
			Group toJoin = joiner.getGroupToJoin(this);
			assert toJoin != null : Group.groups;
			toJoin.joinNode(this.getNode());
			toJoin.updateMembers();

			Debug.debug(this, " joined at " + toJoin);

			if (this.myGroup.size() > GroupReplication.maxReplication) {
				if (mode == Mode.LNLB_PREEMPTIVE) {
					// I should divide
					int stable = 0;
					for (Node i : this.myGroup.getFinger()) {
						GroupReplication p = getProtocol(i);
						if (p.isReliable) {
							stable++;
						}
					}
					boolean isStable = stable > minReplication;
					if (isStable)
						this.myGroup.divide();
					else if (this.myGroup.size() > GroupReplication.maxReplication * 2)
						this.myGroup.divide();
				} else
					this.myGroup.divide();
			}
		}
		if (!(this.myGroup.keys() > 0))
			System.out.println("WARNING: joining a node without keys:" +
					this.getNode() + " " + this.myGroup);
		if (mode == Mode.SCATTER || mode == Mode.LNLB_REBALANCE)
			checkBalance();
		System.out.println("J " + CommonState.getTime() + " " + activeNodes.size() + " " + this.getKeys());
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
		Group getGroupToJoin(GroupReplication n);
	}

	static class JoinAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return GRUtils.getMostAverageLoaded(GRUtils.filterSingleKey(Group.groups));
		}
	}

	static class JoinScatter implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return GRUtils.getMostLoaded(GRUtils.listSmallest(Group.groups));
		}
	}

	static class JoinSmallestAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return GRUtils.getMostAverageLoaded(Group.groups);
		}
	}

	static class JoinPreemptiveAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			// don't join groups which have small load, regardless of their keys
			System.out.println("G:" + Group.groups.size());
			Collection<Group> singleKey = GRUtils.filterSingleKey(Group.groups);
			System.out.println("S:" + singleKey.size());
			List<Group> lst1 = GRUtils.listAboveAverage(GRUtils.listGroupAverageLoads(singleKey));
			System.out.println("L:" + lst1.size());

			if (n.isReliable) {
				Collection<Group> toSelect = GRUtils.sliceInversePercentage(
						GRUtils.listGroupKeys(lst1), slice);
				System.out.println("R:" + toSelect.size());
				return GRUtils.getMostAverageLoaded(toSelect);
			} else {
				Collection<Group> toSelect = GRUtils.slicePercentage(GRUtils.listGroupKeys(lst1), slice);
				System.out.println("N:" + toSelect.size());
				return GRUtils.getMostAverageLoaded(toSelect);
			}
		}
	}

	static class JoinPreemtiveGroup implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			List<Pair<Group, Integer>> lst = GRUtils.listGroupDeaths(Group.groups);
			Collection<Group> lst2 = GRUtils.slicePercentage(lst, 0.5);
			return GRUtils.getMostAverageLoaded(lst2);
		}
	}

	static class JoinPreemtive implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return GRUtils.getNextGroupDeath(Group.groups);
		}
	}

	static class JoinRandom implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return Utils.getRandomEl(Group.groups);
		}
	}

	static class JoinSmallestPreemptive implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return GRUtils.getNextGroupDeath(GRUtils.listSmallest(Group.groups));
		}
	}

	static class JoinSmallest implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return Utils.getRandomEl(GRUtils.listSmallest(Group.groups));
		}
	}

	static class JoinLargest implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication n) {
			return Utils.getRandomEl(GRUtils.listLargest(Group.groups));
		}
	}

	public void killed(Collection<Pair<Node, Integer>> availabilityList) {
		Debug.debug(this, " was killed");

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
		List<Group> lst = GRUtils.listLargest(Group.groups);
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
		GroupReplication.checkIntegrity();
		assert (myNode.getFailState() == Fallible.DOWN);
		myNode.setFailState(Fallible.OK);
		this.deathTime = myDeathTime;
		this.triggerJoin(myNode, myNode);
		GroupReplication.checkIntegrity();
	}

	@Override
	public void kill(Collection<Pair<Node, Integer>> availabilityList) {
		int keys = this.getKeys();
		GroupReplication.checkIntegrity();
		assert (this.getNode().getFailState() == Fallible.OK);
		this.killed(availabilityList);
		this.getNode().setFailState(Fallible.DOWN);
		GroupReplication.checkIntegrity();
		activeNodes.remove(this.getNode());
		System.out.println("K " + CommonState.getTime() + " " + activeNodes.size() + " " + keys);
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
		return this.getNode() + " " + this.myGroup + " " + this.isUp();
	}

	public void setGroup(Group group) {
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
	public int getLoad() {
		return this.myGroup != null ? this.myGroup.load() : 0;
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
		// int keysSum = 0;
		// for (Group it : Group.groups) {
		// keysSum += it.keys();
		// }
		// assert (Group.groups.size() == 0 || keysSum ==
		// GroupReplication.keyCreator.getNKeys()) : keysSum
		// + " " + GroupReplication.keyCreator.getNKeys();
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

	public static TreeSet<Node> getActiveNodes() {
		return activeNodes;
	}

	public boolean isReliable() {
		return isReliable;
	}

	public int getDeathTime() {
		return this.deathTime;
	}
}
