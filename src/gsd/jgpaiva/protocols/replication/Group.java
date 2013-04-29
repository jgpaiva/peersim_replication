package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.observers.Debug;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.protocols.replication.GroupReplication.Mode;
import gsd.jgpaiva.structures.dht.FingerGroup;
import gsd.jgpaiva.utils.Identifier;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.Utils;

import java.math.BigInteger;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;

import peersim.core.CommonState;
import peersim.core.Node;

public class Group {
	public static final HashSet<Group> groups = new HashSet<Group>();
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
				GroupReplication.getProtocol(it).sendMessage(
						new GroupReplication.KeyTransferMessage(moved));
			}
			return moved;
		} else if (1.6 * this.successor.keys < this.keys) {
			int totalKeys = successor.keys + this.keys;
			int moved = totalKeys / 2 - this.successor.keys;
			this.successor.keys += moved;
			this.keys = totalKeys - this.successor.keys;

			for (Node it : this.successor.finger) {
				GroupReplication.getProtocol(it).sendMessage(
						new GroupReplication.KeyTransferMessage(moved));
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
		Group mergeTo = mergeP.getGroup(this);
		Debug.debug(this, " will merge to:" + mergeTo);
		Group res = mergeTo(mergeTo);
		Debug.debug(this, " merged into: " + res + "\nDEBUG groups: " + groups);
	}

	public void divide() {
		Group divideTo = divideP.getGroup(this);
		Debug.debug(this, " will divide to:" + divideTo);
		Group res = divideTo(divideTo);
		Debug.debug(this, " divided and created: " + res + "\nDEBUG groups: " + groups);
	}

	private Group mergeTo(Group mergeTo) {
		GroupReplication.checkIntegrity();
		if (Group.groups.size() == 1)
			return null;
		assert (this.successor != null);
		Group.mergesCount++;

		mergeKeysTo(mergeTo);
		mergeNodesTo(mergeTo);

		GroupReplication.checkIntegrity();
		return mergeTo;
	}

	private void mergeKeysTo(Group mergeTo) {
		int oldKeys = this.keys;
		int mergeToKeys = mergeTo.keys;
		mergeTo.keys = mergeToKeys + oldKeys;
		mergeTo.load = this.load + mergeTo.load;
		assert (oldKeys >= 0 && mergeToKeys >= 0) : oldKeys + " " + mergeToKeys;
		for (Node it : this.successor.finger) {
			GroupReplication.getProtocol(it).sendMessage(
					new GroupReplication.KeyTransferMessage(oldKeys));
		}
		for (Node it : this.finger) {
			GroupReplication.getProtocol(it).sendMessage(
					new GroupReplication.KeyTransferMessage(mergeToKeys));
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

	private Group divideTo(Group divideTo) {
		Group.divisionsCount++;
		if (divideTo == null)
			divideTo = this;

		Group newGroup = splitIntoNewGroup();
		divideTo.moveKeysTo(newGroup);
		divideTo.setAsPredecessor(newGroup);
		return newGroup;
	}

	private Group splitIntoNewGroup() {
		Group newGroup = null;
		TreeSet<Node> setNew = new TreeSet<Node>();
		int newSize = this.finger.size() / 2;
		int oldSize = this.finger.size() - newSize;

		assert (oldSize >= newSize) : oldSize + " " + newSize + " " +
				this.finger.size();
		assert (oldSize >= 0) : oldSize + " " + newSize;

		if (GroupReplication.mode == Mode.LNLB_PREEMPTIVE) {// ||
																// GroupReplication2.mode
																// == Mode.LNLB)
																// {
			while (this.finger.size() > oldSize) {
				Node n = GRUtils.getMinDeath(this.finger);
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
			System.out.println("WARNING: group with zero keys: newKeys:" + newKeys + " oldKeys:" + oldKeys
					+ " initialSize:" + totalSize + " newSize:" + newSize);
		}
		if (!(newLoad > 0 && oldLoad > 0)) {
			System.out.println("WARNING: group with zero load: newLoad:" + newLoad + " oldLoad:" + oldLoad
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
			int newLoad = GroupReplication.unevenLoad ? group.load / 100 : newKeys;
			return new Pair<Integer, Integer>(newKeys, newLoad);
		}

		static RangeSpliter instance = new RangeSpliter();
	}

	public static class LoadSpliter implements KeyRangeBreaker {
		@Override
		public Pair<Integer, Integer> getNewKeys(final int initialSize, final int newSize, Group group) {
			int newLoad = (int) (group.load / (((double) initialSize) / ((double) newSize)));
			int newKeys = (int) (GroupReplication.unevenLoad ? Math.ceil(group.keys) * 0.10 : group.load);
			return new Pair<Integer, Integer>(newKeys, newLoad);
		}

		static LoadSpliter instance = new LoadSpliter();
	}

	public interface GroupPicker {
		public Group getGroup(Group g);
	}

	public static class SuccessorPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			return g.successor;
		}

		static SuccessorPicker instance = new SuccessorPicker();
	}

	static class LoadedPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			if (Group.groups.size() == 1)
				return null;
			Group mostLoaded = null;
			for (Group i : Group.groups) {
				if (i != g) {
					if (mostLoaded == null
							|| GRUtils.getAvgGroupLoad(i) > GRUtils.getAvgGroupLoad(mostLoaded))
						mostLoaded = i;
				}
			}
			return mostLoaded;
		}

		static LoadedPicker instance = new LoadedPicker();
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

		static LargestPicker instance = new LargestPicker();
	}

	static class ThisPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			return g;
		}

		static ThisPicker instance = new ThisPicker();
	}

	static class RandomPicker implements GroupPicker {
		@Override
		public Group getGroup(Group g) {
			return Utils.getRandomEl(Group.groups);
		}

		static RandomPicker instance = new RandomPicker();
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

		static RandomNotThisPicker instance = new RandomNotThisPicker();
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
					}
				});

		for (Node it : set) {
			candidates.add(it);
		}
		assert (set.size() == candidates.size());
		return candidates;
	}

	public void updateMembers() {
		for (Node it : this.finger) {
			GroupReplication.getProtocol(it).setGroup(this);
		}
	}

	void joinNode(Node node) {
		Group.joinCount++;
		this.finger.add(node);
		GroupReplication.getProtocol(node).sendMessage(
				new GroupReplication.KeyTransferMessage(this.keys));
	}

	void removeNode(Node node) {
		Group.leaveCount++;
		this.finger.remove(node);
		GroupReplication.getProtocol(node).setGroup(null);
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

	public TreeSet<Node> getFinger() {
		return this.finger;
	}

	@Override
	public String toString() {
		return "L:" + this.load + "K:" + this.keys + "" + this.finger + "";
	}
}