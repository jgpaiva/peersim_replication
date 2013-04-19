package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.interfaces.ChainReplProtocol;
import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.GroupSizeObservable;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.Utils;

import java.math.BigInteger;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import peersim.config.Configuration;
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
	private static final String PAR_SLICE = "slice";
	private static final String PAR_ABOVEAVG = "aboveavg";
	private static final String PAR_KEYS_BEFORE_LOAD = "keysbeforeload";

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
	private static double slice;
	private static double aboveAvg;
	private static boolean keysBeforeLoad;
	private static Joiner joiner;

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

		if (mode == Mode.LNLB_PREEMPTIVE)
			setMode(Mode.LNLB);
		else
			setMode(mode);
	}

	public void setMode(Mode mode) {
		System.err.println("Working in " + mode + " mode");

		if (mode == Mode.LNLB_PREEMPTIVE) {
			GroupReplication2.slice = Configuration.getDouble(name + '.'
					+ GroupReplication2.PAR_SLICE);
			GroupReplication2.aboveAvg = Configuration.getDouble(name + '.'
					+ GroupReplication2.PAR_ABOVEAVG);
			GroupReplication2.keysBeforeLoad = Configuration.getBoolean(name + '.'
					+ GroupReplication2.PAR_KEYS_BEFORE_LOAD);
			System.err.println("Slice at:" + slice + " Above average at:" + aboveAvg + " Keys before load? "
					+ keysBeforeLoad);
		}

		if (mode == Mode.LNLB || mode == Mode.LNLB_SUPERSIZE || mode == Mode.LNLB_RESCUE
				|| mode == Mode.LNLB_MERGE || mode == Mode.LNLB_REBALANCE)
			GroupReplication2.joiner = new JoinAverageMostLoaded();
		if (mode == Mode.LNLB_PREEMPTIVE)
			GroupReplication2.joiner = new JoinPreemptiveAverageMostLoaded();
		else if (mode == Mode.LNLB_SMALLEST)
			GroupReplication2.joiner = new JoinSmallestAverageMostLoaded();
		else if (mode == Mode.SCATTER || mode == Mode.SCATTER_NOREBALANCE)
			GroupReplication2.joiner = new JoinScatter();
		else if (mode == Mode.PREEMPTIVE)
			GroupReplication2.joiner = new JoinPreemtive();
		else if (mode == Mode.PREEMPTIVE_GROUP)
			GroupReplication2.joiner = new JoinPreemtiveGroup();
		else if (mode == Mode.SUPERSIZE)
			GroupReplication2.joiner = new JoinSmallest();
		else if (mode == Mode.SURPLUS)
			GroupReplication2.joiner = new JoinLargest();
		else if (mode == Mode.RANDOM || mode == Mode.RANDOM2 || mode == Mode.RANDOM3)
			GroupReplication2.joiner = new JoinRandom();
		else if (mode == Mode.SMALLEST_PREEMPTIVE)
			GroupReplication2.joiner = new JoinSmallestPreemptive();

		if (mode == Mode.LNLB || mode == Mode.RANDOM3 || mode == Mode.LNLB_SMALLEST
				|| mode == Mode.LNLB_REBALANCE || mode == Mode.PREEMPTIVE_GROUP
				|| mode == Mode.LNLB_SUPERSIZE || mode == Mode.LNLB_RESCUE) {
			Group.keyRangeBreaker = Group.LoadSpliter.instance;
		} else {
			Group.keyRangeBreaker = Group.RangeSpliter.instance;
		}

		if (mode == Mode.RANDOM) {
			Group.mergeP = Group.RandomNotThisPicker.instance;
		} else if (mode == Mode.SURPLUS) {
			Group.mergeP = Group.LargestPicker.instance;
		} else if (mode == Mode.LNLB_MERGE) {
			Group.mergeP = Group.LoadedPicker.instance;
		} else if (mode == Mode.RANDOM2) {
			Group.mergeP = Group.RandomNotThisPicker.instance;
		} else {
			Group.mergeP = Group.SuccessorPicker.instance;
		}

		if (mode == Mode.RANDOM) {
			Group.divideP = Group.RandomPicker.instance;
		} else {
			Group.divideP = Group.ThisPicker.instance;
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
			Group toJoin = joiner.getGroupToJoin(this);
			assert toJoin != null : Group.groups;
			toJoin.joinNode(this.getNode());
			toJoin.updateMembers();

			if (this.myGroup.size() > GroupReplication2.maxReplication) {
				this.myGroup.divide();
			}
		}
		if (!(this.myGroup.keys() > 0))
			System.out.println("WARNING: joining a node without keys:" +
					this.getNode() + " " + this.myGroup);
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
			return GRUtils.getMostLoaded(GRUtils.listSmallest(Group.groups));
		}
	}

	class JoinSmallestAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getMostAverageLoaded(Group.groups);
		}
	}

	class JoinPreemptiveAverageMostLoaded implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			boolean isAboveAvg = GRUtils.isInPercDeathTime(n.getNode(), Group.groups, aboveAvg);

			if (keysBeforeLoad) {
				if (isAboveAvg) {
					Collection<Group> toSelect = GRUtils.slicePercentage(GRUtils.listGroupKeys(Group.groups),
							slice);
					return GRUtils.getMostAverageLoaded(toSelect);
				} else {
					Collection<Group> toSelect = GRUtils.sliceInversePercentage(
							GRUtils.listGroupKeys(Group.groups), slice);
					return GRUtils.getMostAverageLoaded(toSelect);
				}
			} else {
				if (isAboveAvg) {
					Collection<Group> toSelect = GRUtils.slicePercentage(
							GRUtils.listGroupAverageLoads(Group.groups),
							slice);
					return GRUtils.getMostKeys(toSelect);
				} else {
					Collection<Group> toSelect = GRUtils.sliceInversePercentage(
							GRUtils.listGroupAverageLoads(Group.groups),
							slice);
					return GRUtils.getMostKeys(toSelect);
				}
			}
		}
	}

	class JoinPreemtiveGroup implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			List<Pair<Group, Integer>> lst = GRUtils.listGroupDeaths(Group.groups);
			Collection<Group> lst2 = GRUtils.slicePercentage(lst, 0.5);
			return GRUtils.getMostAverageLoaded(lst2);
		}
	}

	class JoinPreemtive implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return GRUtils.getNextGroupDeath(Group.groups);
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
			return GRUtils.getNextGroupDeath(GRUtils.listSmallest(Group.groups));
		}
	}

	class JoinSmallest implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return Utils.getRandomEl(GRUtils.listSmallest(Group.groups));
		}
	}

	class JoinLargest implements Joiner {
		@Override
		public Group getGroupToJoin(GroupReplication2 n) {
			return Utils.getRandomEl(GRUtils.listLargest(Group.groups));
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
		GroupReplication2.checkIntegrity();
		assert (myNode.getFailState() == Fallible.DOWN);
		myNode.setFailState(Fallible.OK);
		this.deathTime = myDeathTime;
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
