package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.observers.Debug;
import gsd.jgpaiva.structures.replication.Key;
import gsd.jgpaiva.utils.IncrementalFreq;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.Utils;

import java.util.HashSet;
import java.util.Iterator;
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
	static GroupSplitter groupSplitter;

	// shortcut
	private static Integer totalKeys = null;
	private static KeyCreator keyCreator;
	// group state variables
	TreeSet<Node> finger;
	Group successor;
	private Group predecessor;

	int keys = 0;
	int load = 0;

	// intervals in the key array corresponding to this node's keys. keyBott is
	// not included in interval
	int keyCeil;
	int keyBott;

	public int checkBalance() {
		// avoid corner cases
		if (this.successor == null || this.keys() < 2 || this.successor.keys() < 2)
			return 0;

		if (this.successor.load() > 1.6 * this.load()) {
			int totalLoad = successor.load() + this.load();
			int toMove = totalLoad / 2 - this.load();

			int accum = 0;
			int newCeil = -1;
			int keyCount = 0;
			Key[] keyArray = keyCreator.getKeyArray();
			Iterator<Integer> it = GRUtils.circularIterForward(keyArray, this.keyCeil);
			while (accum < toMove) {
				newCeil = it.next();
				accum += keyArray[newCeil].load;
				keyCount++;
			}

			// avoid cycles trying to move a single key in ping pong
			if (keyCount < 2)
				return 0;

			this.setKeys(this.keys() + keyCount);
			this.successor.setKeys(this.successor.keys() - keyCount);
			this.setLoad(this.load() + accum);
			this.successor.setLoad(this.successor.load() - accum);

			this.keyCeil = newCeil;
			this.successor.keyBott = newCeil;

			for (Node it2 : this.finger) {
				GroupReplication.getProtocol(it2).sendMessage(
						new GroupReplication.KeyTransferMessage(keyCount));
			}
			assert GRUtils.calculateIntervalSize(totalKeys, this.keyBott, this.keyCeil) == this.keys() : GRUtils
					.calculateIntervalSize(totalKeys, this.keyBott, this.keyCeil)
					+ " " + this.keys();
			assert GRUtils.calculateIntervalSize(totalKeys, this.successor.keyBott, this.successor.keyCeil) == this.successor
					.keys() : GRUtils.calculateIntervalSize(totalKeys, this.successor.keyBott,
					this.successor.keyCeil) + " " + this.successor.keys();

			return keyCount;
		} else if (1.6 * this.successor.load() < this.load()) {
			int totalLoad = successor.load() + this.load();
			int toMove = totalLoad / 2 - this.successor.load();

			int accum = 0;
			int newCeil = -1;
			int keyCount = 0;
			Key[] keyArray = keyCreator.getKeyArray();
			Iterator<Integer> it = GRUtils.circularIterBackward(keyArray, this.keyCeil);
			while (accum < toMove) {
				accum += keyArray[it.next()].load;
				keyCount++;
			}
			// Ceil pointer is included in interval, must advance one more
			newCeil = it.next();

			// avoid cycles trying to move a single key in ping pong
			if (keyCount < 2)
				return 0;

			this.setKeys(this.keys() - keyCount);
			this.successor.setKeys(this.successor.keys() + keyCount);
			this.setLoad(this.load() - accum);
			this.successor.setLoad(this.successor.load() + accum);

			this.keyCeil = newCeil;
			this.successor.keyBott = newCeil;

			for (Node it2 : this.successor.finger) {
				GroupReplication.getProtocol(it2).sendMessage(
						new GroupReplication.KeyTransferMessage(keyCount));
			}

			assert GRUtils.calculateIntervalSize(totalKeys, this.keyBott, this.keyCeil) == this.keys() : GRUtils
					.calculateIntervalSize(totalKeys, this.keyBott, this.keyCeil)
					+ " " + this.keys();
			assert GRUtils.calculateIntervalSize(totalKeys, this.successor.keyBott, this.successor.keyCeil) == this.successor
					.keys() : GRUtils.calculateIntervalSize(totalKeys, this.successor.keyBott,
					this.successor.keyCeil) + " " + this.successor.keys();
			return keyCount;
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
		toReturn.finger = set;
		keyCreator = GroupReplication.keyCreator;
		Group.totalKeys = keyCreator.getTotalKeys();
		toReturn.setKeys(keyCreator.getNKeys());
		toReturn.setLoad(keyCreator.getTotalLoad());
		// -1 means match all
		toReturn.keyBott = -1;
		toReturn.keyCeil = -1;
		assert GRUtils.calculateIntervalSize(totalKeys, toReturn.keyBott, toReturn.keyCeil) == toReturn
				.keys() : GRUtils.calculateIntervalSize(totalKeys, toReturn.keyBott, toReturn.keyCeil) + " "
				+ toReturn.keys();
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
		int oldKeys = this.keys();
		int mergeToKeys = mergeTo.keys();
		mergeTo.setKeys(mergeToKeys + oldKeys);
		mergeTo.setLoad(this.load() + mergeTo.load());
		// update key pointers
		mergeTo.keyBott = this.keyBott;
		if (mergeTo.keyBott == mergeTo.keyCeil) {
			mergeTo.keyBott = -1;
			mergeTo.keyCeil = -1;
		}
		assert GRUtils.calculateIntervalSize(totalKeys, mergeTo.keyBott, mergeTo.keyCeil) == mergeTo.keys() : GRUtils
				.calculateIntervalSize(totalKeys, mergeTo.keyBott, mergeTo.keyCeil)
				+ " " + mergeTo.keys();

		assert (oldKeys >= 0 && mergeToKeys >= 0) : oldKeys + " " + mergeToKeys;
		for (Node it : this.successor.finger) {
			GroupReplication.getProtocol(it).sendMessage(
					new GroupReplication.KeyTransferMessage(oldKeys));
		}
		for (Node it : this.finger) {
			GroupReplication.getProtocol(it).sendMessage(
					new GroupReplication.KeyTransferMessage(mergeToKeys));
		}

		int tKeys = this.successor.finger.size() * oldKeys + this.finger.size() * mergeToKeys;
		System.out.println("M " + CommonState.getTime() + " " + GroupReplication.getActiveNodes().size()
				+ " " + tKeys);
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

	public interface GroupSplitter {
		TreeSet<Node> split(int newSize, int oldSize, Group toSplit);
	}

	private Group splitIntoNewGroup() {
		Group newGroup = null;
		TreeSet<Node> setNew = new TreeSet<Node>();
		int newSize = this.finger.size() / 2;
		int oldSize = this.finger.size() - newSize;

		assert (oldSize >= newSize) : oldSize + " " + newSize + " " +
				this.finger.size();
		assert (oldSize >= 0) : oldSize + " " + newSize;

		setNew = groupSplitter.split(newSize, oldSize, this);

		newGroup = Group.createGroup(setNew);

		newGroup.updateMembers();
		this.updateMembers();
		return newGroup;
	}

	public static class ReliabilitySplit implements GroupSplitter {
		@Override
		public TreeSet<Node> split(int newSize, int oldSize, Group toSplit) {
			TreeSet<Node> retVal = new TreeSet<Node>();
			TreeSet<Node> reliable = new TreeSet<Node>();
			TreeSet<Node> unReliable = new TreeSet<Node>();

			for (Node it : toSplit.finger) {
				GroupReplication p = GroupReplication.getProtocol(it);
				if (p.isReliable())
					reliable.add(p.getNode());
				else
					unReliable.add(p.getNode());
			}

			while (toSplit.finger.size() > oldSize) {
				Node n;
				if (unReliable.size() > 0)
					n = Utils.removeRandomEl(unReliable);
				else
					n = Utils.removeRandomEl(reliable);
				toSplit.finger.remove(n);
				retVal.add(n);
			}
			return retVal;
		}

		public static GroupSplitter instance = new ReliabilitySplit();
	}

	public static class RandomSplit implements GroupSplitter {
		@Override
		public TreeSet<Node> split(int newSize, int oldSize, Group toSplit) {
			TreeSet<Node> retVal = new TreeSet<Node>();
			while (toSplit.finger.size() > oldSize) {
				retVal.add(Utils.removeRandomEl(toSplit.finger));
			}
			return retVal;
		}

		public static GroupSplitter instance = new RandomSplit();
	}

	private void moveKeysTo(Group newGroup) {
		final int totalSize = newGroup.size() + this.size();
		final int newSize = newGroup.size();

		Group.keyRangeBreaker.getNewKeys(totalSize, newSize, this, newGroup, keyCreator.getKeyArray());

		if (!(newGroup.keys() > 0 && this.keys() > 0)) {
			System.out.println("WARNING: group with zero keys: newKeys:" + newGroup.keys() + " oldKeys:"
					+ this.keys() + " initialSize:" + totalSize + " newSize:" + newSize);
		}
		if (!(newGroup.load() > 0 && this.load() > 0)) {
			System.out.println("WARNING: group with zero load: newLoad:" + newGroup.load() + " oldLoad:"
					+ this.load() + " initialSize:" + totalSize + " newSize:" + newSize);
		}
		int tKeys = this.keys() * this.size() + newGroup.keys() * newGroup.size();
		System.out.println("D " + CommonState.getTime() + " " + GroupReplication.getActiveNodes().size()
				+ " " + tKeys);
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
		void getNewKeys(final int initialSize, final int newSize, Group oldGroup, Group newGroup,
				Key[] keyArray);
	}

	public static class LoadSpliter implements KeyRangeBreaker {
		@Override
		public void getNewKeys(int initialSize, int newSize, Group oldGroup, Group newGroup, Key[] keyArray) {
			if (oldGroup.keys() == 1) {
				throw new RuntimeException("Cannot divide anymore");
			}

			int newLoad = (int) (oldGroup.load() / (((double) initialSize) / ((double) newSize)));
			Iterator<Integer> it = GRUtils.circularIterForward(keyArray, oldGroup.keyBott);
			int accum = 0;
			int newCeil = -1;
			int keyCount = 0;
			while (accum <= newLoad) {
				newCeil = it.next();
				accum += keyArray[newCeil].load;
				keyCount++;
			}

			if (keyCount == oldGroup.keys()) { // rollback one
				it = GRUtils.circularIterBackward(keyArray, newCeil);
				accum -= keyArray[it.next()].load;
				newCeil = it.next();
				keyCount--;
			}

			newGroup.keyBott = oldGroup.keyBott;
			newGroup.keyCeil = newCeil;
			oldGroup.keyBott = newCeil;
			if (oldGroup.keyCeil == -1) {
				oldGroup.keyCeil = keyArray.length - 1;
				newGroup.keyBott = oldGroup.keyCeil;
			}

			oldGroup.setLoad(oldGroup.load() - accum);
			newGroup.setLoad(accum);
			oldGroup.setKeys(oldGroup.keys() - keyCount);
			newGroup.setKeys(keyCount);

			assert newGroup.keys() > 0 : newLoad + " " + keyCount + " " + accum + " "
					+ initialSize + " " + newSize;
			assert oldGroup.keys() > 0 : newLoad + " " + keyCount + " " + accum + " "
					+ initialSize + " " + newSize;

			assert GRUtils.calculateIntervalSize(totalKeys, newGroup.keyBott, newGroup.keyCeil) == newGroup
					.keys() : GRUtils.calculateIntervalSize(totalKeys, newGroup.keyBott, newGroup.keyCeil)
					+ " " + newGroup.keys();
			assert GRUtils.calculateIntervalSize(totalKeys, oldGroup.keyBott, oldGroup.keyCeil) == oldGroup
					.keys() : GRUtils.calculateIntervalSize(totalKeys, oldGroup.keyBott, oldGroup.keyCeil)
					+ " " + oldGroup.keys();
		}

		static LoadSpliter instance = new LoadSpliter();
	}

	public static class LoadAndResortSpliter implements KeyRangeBreaker {
		@Override
		public void getNewKeys(int initialSize, int newSize, Group oldGroup, Group newGroup, Key[] keyArray) {
			LoadSpliter.instance.getNewKeys(initialSize, newSize, oldGroup, newGroup, keyArray);

			// after moving keys around, move nodes to match the keys
			if (oldGroup.keys() > newGroup.keys()) {
				oldGroup.switchGroupMembers(newGroup);
			}
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
			Group toMerge = g;
			while (toMerge == g)
				toMerge = Utils.getRandomEl(Group.groups);
			return toMerge;
		}

		static RandomNotThisPicker instance = new RandomNotThisPicker();
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
				new GroupReplication.KeyTransferMessage(this.keys()));
	}

	void removeNode(Node node) {
		Group.leaveCount++;
		this.finger.remove(node);
		GroupReplication.getProtocol(node).setGroup(null);
	}

	public void switchGroupMembers(Group newGroup) {
		TreeSet<Node> tmp = this.finger;
		this.finger = newGroup.finger;
		newGroup.finger = tmp;
		this.updateMembers();
		newGroup.updateMembers();
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

	void setLoad(int l) {
		this.load = l;
	}

	void setKeys(int k) {
		this.keys = k;
	}

	int load() {
		return this.load;
	}

	public int size() {
		return this.finger.size();
	}

	public TreeSet<Node> getFinger() {
		return this.finger;
	}

	@Override
	public String toString() {
		return "L:" + this.load() + "K:" + this.keys() + "" + this.finger + " " + this.reliableCount();
	}

	private String reliableCount() {
		int count = 0;
		int avgDeath = 0;
		for (Node i : this.finger) {
			GroupReplication p = (GroupReplication) i.getProtocol(GroupReplication.getPID());
			if (p.isReliable()) {
				count++;
			}
			avgDeath += p.deathTime;
		}
		return count + "/" + this.finger.size() + " D:" + (((double) avgDeath) / this.size());
	}

	public String dumpLoads() {
		IncrementalFreq f = new IncrementalFreq();
		Key[] arr = keyCreator.getKeyArray();
		if (keyBott == keyCeil)
			return "ALL";

		Iterator<Integer> it = GRUtils.circularIterForward(arr, keyBott);
		while (true) {
			Integer cur = it.next();
			f.add(arr[cur].load);
			if (cur == keyCeil)
				break;
		}
		return f.toString();
	}
}