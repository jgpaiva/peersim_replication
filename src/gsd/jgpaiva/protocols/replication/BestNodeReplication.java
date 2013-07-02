package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.interfaces.ChainReplProtocol;
import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import gsd.jgpaiva.interfaces.RunnableWithArgs;
import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.structures.dht.Finger;
import gsd.jgpaiva.structures.replication.ComplexKey;
import gsd.jgpaiva.structures.replication.Key;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.Identifier;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.KeyCreator.KeyMode;
import gsd.jgpaiva.utils.MyStore;
import gsd.jgpaiva.utils.MyStoreIterator;
import gsd.jgpaiva.utils.Pair;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Fallible;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.Protocol;

public class BestNodeReplication extends ProtocolStub implements Protocol, UptimeSimulatorProtocol,
		KeyStorageProtocol, MonitorableProtocol, ChainReplProtocol {
	private static final String PAR_WINDOW = "window";
	private static final String PAR_KEEP_AT_SUCCESSOR = "keepatsuccessor";
	private static final String PAR_USE_BEST_NODES = "usebestnodes";
	private static final String PAR_ERROR = "error";

	private static int idLength;
	private static int window;
	public static KeyCreator keyCreator;
	private static int replication;
	private static boolean keepAtSuccessorWindow;
	public final static TreeSet<Finger> activeNodes = new TreeSet<Finger>();
	private static boolean useBestNodes;
	private static double error;
	static boolean simStarted = false;

	private MyStore<ComplexKey> myKeys;
	private MyStore<ComplexKey> myReplicatedKeys;

	private Finger finger;
	private Finger successor;
	private Finger predecessor;
	private Integer deathTime;

	public BestNodeReplication(String prefix) {
		super(prefix);
		BestNodeReplication.idLength = GlobalConfig.getIdLength();
		BestNodeReplication.replication = GlobalConfig.getReplication();
		BestNodeReplication.window = Configuration.getInt(ProtocolStub.name + "."
				+ BestNodeReplication.PAR_WINDOW);
		BestNodeReplication.keepAtSuccessorWindow = Configuration.getBoolean(ProtocolStub.name
				+ "." + BestNodeReplication.PAR_KEEP_AT_SUCCESSOR);
		BestNodeReplication.useBestNodes = Configuration.getBoolean(ProtocolStub.name + "."
				+ BestNodeReplication.PAR_USE_BEST_NODES);
		if (Configuration.contains(prefix + "." + BestNodeReplication.PAR_ERROR)) {
			BestNodeReplication.error = Configuration.getDouble(prefix + "."
					+ BestNodeReplication.PAR_ERROR);
		} else {
			BestNodeReplication.error = 0;
		}
		this.myKeys = new MyStore<ComplexKey>();
		this.myReplicatedKeys = new MyStore<ComplexKey>();
		keyCreator = KeyCreator.initialize(KeyMode.COMPLEX_KEY);
	}

	@Override
	public BestNodeReplication clone() {
		BestNodeReplication c = (BestNodeReplication) super.clone();
		c = (BestNodeReplication) super.clone();
		c.myKeys = new MyStore<ComplexKey>();
		c.myReplicatedKeys = new MyStore<ComplexKey>();
		return c;
	}

	@Override
	public boolean shouldPrint() {
		return true;
	}

	BestNodeReplication getProtocol(Node node) {
		BestNodeReplication val = (BestNodeReplication) node.getProtocol(ProtocolStub.pid);
		return val;
	}

	@Override
	public void join(Node contact) {
		this.finger = new Finger(this.getNode(), new Identifier(new BigInteger(
				BestNodeReplication.idLength, CommonState.r)));
		// step 1: get successor and set as predecessor
		if (BestNodeReplication.activeNodes.size() == 0) {
			this.setSuccessor(null);
			this.setPredecessor(null);
		} else {
			Finger successor = BestNodeReplication.activeNodes.ceiling(this.finger);
			if (successor == null) {
				successor = BestNodeReplication.activeNodes.first();
			}
			this.setSuccessor(successor);
			Finger oldPred = this.getProtocol(successor.n).setPredecessor(this.finger);
			if (oldPred == null) {
				Finger val = this.getProtocol(successor.n).setSuccessor(this.finger);
				assert (val == null) : this.finger + " " + this.successor + " " + this.predecessor
						+ " " + BestNodeReplication.activeNodes;
				this.setPredecessor(successor);
			} else {
				Finger val = this.getProtocol(oldPred.n).setSuccessor(this.finger);
				assert (val == this.getSuccessor());
				this.setPredecessor(oldPred);
			}
		}

		if (BestNodeReplication.simStarted) {
			// get the keys that should be mine
			BestNodeReplication proto = this.getProtocol(this.getSuccessor().n);

			assert (proto.myKeys.size() == BestNodeReplication.keyCreator.getRangeSize(
					this.getPredecessor(), proto.finger));

			// removed will contain keys that must be re-replicated
			Collection<? extends Key> top = BestNodeReplication.keyCreator.getRangeTop(
					this.getPredecessor(), this.finger);
			Collection<? extends Key> bottom = BestNodeReplication.keyCreator.getRangeBottom(
					this.getPredecessor(), this.finger);

			int initialOtherSize = proto.myKeys.size();
			proto.myKeys.removeAll(bottom);
			proto.myKeys.removeAll(top);
			assert (proto.myKeys.size() == initialOtherSize - bottom.size() - top.size());

			for (Key it : bottom) {
				ComplexKey theKey = (ComplexKey) it;
				this.myKeys.add(theKey);
				assert (theKey.ownerNode == proto.getNode());
				theKey.ownerNode = this.getNode();
			}
			for (Key it : top) {
				ComplexKey theKey = (ComplexKey) it;
				this.myKeys.add(theKey);
				assert (theKey.ownerNode == proto.getNode());
				theKey.ownerNode = this.getNode();
			}

			if (BestNodeReplication.keepAtSuccessorWindow) {
				// move successor keys to correct placement
				BestNodeReplication toRemove = BestNodeReplication.getNPredecessor(this,
						BestNodeReplication.window);
				BestNodeReplication succ = this.getProtocol(this.getSuccessor().n);
				MyStore<ComplexKey> successorKeys = succ.myReplicatedKeys;
				MyStore<ComplexKey> result = successorKeys.removeAllMatching(toRemove.myKeys);
				int moved = toRemove.reReplicate(succ.getNode(), result);
				this.addCost(new KeyTransferMessage(moved));
			}
		}

		BestNodeReplication.activeNodes.add(this.finger);
	}

	public void killed() {
		BestNodeReplication.activeNodes.remove(this.finger);
		this.finger = null;

		// step1: remove my node from topology
		if (this.getSuccessor() == this.getPredecessor()) {
			if (this.getSuccessor() == null)
				return;
			this.getProtocol(this.getSuccessor().n).setPredecessor(null);
			this.getProtocol(this.getSuccessor().n).setSuccessor(null);
		} else {
			this.getProtocol(this.getSuccessor().n).setPredecessor(this.getPredecessor());
			this.getProtocol(this.getPredecessor().n).setSuccessor(this.getSuccessor());
		}

		if (BestNodeReplication.simStarted) {
			// step2 : move my keys to my successor
			assert (this.getSuccessor() != null);
			BestNodeReplication proto = this.getProtocol(this.getSuccessor().n);
			// removed will contain keys that must be re-replicated
			int myKeysInitialSize = this.myKeys.size();
			MyStore<ComplexKey> removed = proto.myReplicatedKeys.removeAllMatching(this.myKeys);
			// keys sent to successor i.e.could not be obtained locally
			int moved = myKeysInitialSize - removed.size();
			for (ComplexKey it : this.myKeys) {
				it.ownerNode = proto.getNode();
			}
			int ret = proto.myKeys.moveToMe(this.myKeys);
			assert (ret == myKeysInitialSize);

			moved += proto.reReplicate(proto.getNode(), removed);

			// step3: re-replicate the keys I was replicating
			int myReplicatedKeysInitialSize = this.myReplicatedKeys.size();
			MyStoreIterator<ComplexKey> it = (MyStoreIterator<ComplexKey>) this.myReplicatedKeys
					.iterator();
			HashMap<Node, MyStore<ComplexKey>> map = new HashMap<Node, MyStore<ComplexKey>>();
			while (it.hasNext()) {
				ComplexKey k = it.next();
				MyStore<ComplexKey> res = map.get(k.ownerNode);
				if (res == null) {
					res = new MyStore<ComplexKey>();
					map.put(k.ownerNode, res);
				}
				it.moveTo(res);
			}
			int reReplicated = 0;
			for (Entry<Node, MyStore<ComplexKey>> it2 : map.entrySet()) {
				proto = this.getProtocol(it2.getKey());
				reReplicated += proto.reReplicate(this.getNode(), it2.getValue());
			}
			this.addCost(new KeyTransferMessage(reReplicated));
			this.addCost(new KeyTransferMessage(moved));
			assert (moved == myKeysInitialSize);
			assert (reReplicated == myReplicatedKeysInitialSize);
			assert (this.myKeys.size() == 0);
			assert (this.myReplicatedKeys.size() == 0);
		}
	}

	private int reReplicate(final Node toReplace, MyStore<ComplexKey> removed) {
		int replicated = 0;
		int initialRemovedSize = removed.size();
		Collection<BestNodeReplication> candidates = this.getCandidates();

		for (BestNodeReplication it : candidates) {
			final BestNodeReplication val = it;
			final RunnableWithArgs funct = new RunnableWithArgs() {
				final Node oldReplica = toReplace;
				final Node newReplica = val.getNode();

				@Override
				public final void run(Object arg) {
					ComplexKey key = (ComplexKey) arg;
					BestNodeReplication.changeReplica(key, this.oldReplica, this.newReplica);
				}
			};
			replicated += it.myReplicatedKeys.moveToMe(removed, funct);
			if (removed.size() == 0) {
				break;
			}
		}
		assert (removed.size() == 0 && replicated == initialRemovedSize);
		return replicated;
	}

	public static final void changeReplica(ComplexKey key, Node oldReplica, Node newReplica) {
		Node[] replicas = key.replicas;
		for (int it = 0; it < replicas.length; it++) {
			assert (replicas[it] != null);
			if (replicas[it] == oldReplica) {
				replicas[it] = newReplica;
				return;
			}
		}
		throw new RuntimeException("Should never be reached" + Arrays.toString(replicas));
	}

	private int replicate(MyStore<ComplexKey> keys) {
		int replicated = 0;
		Collection<BestNodeReplication> candidates = this.getCandidates();

		int counter = 0;
		for (BestNodeReplication it : candidates) {
			if (counter >= BestNodeReplication.replication - 1) {
				break;
			}
			counter++;

			final BestNodeReplication val = it;
			final RunnableWithArgs funct = new RunnableWithArgs() {
				final Node newReplica = val.getNode();

				@Override
				public final void run(Object arg) {
					ComplexKey key = (ComplexKey) arg;
					BestNodeReplication.createReplica(key, this.newReplica);
				}
			};
			replicated += it.myReplicatedKeys.addAll(keys, funct);
		}
		assert (replicated == keys.size() * (BestNodeReplication.replication - 1));
		return replicated;
	}

	public static final void createReplica(ComplexKey key, Node newReplica) {
		if (key.replicas == null) {
			key.replicas = new Node[BestNodeReplication.replication - 1];
		}
		Node[] replicas = key.replicas;
		for (int it = 0; it < replicas.length; it++) {
			if (replicas[it] == null) {
				replicas[it] = newReplica;
				return;
			}
		}
		throw new RuntimeException("Should never be reached");
	}

	private Collection<BestNodeReplication> getCandidates() {
		Collection<BestNodeReplication> candidates;
		if (BestNodeReplication.useBestNodes) {
			// candidates sorted by descending death time
			candidates = new TreeSet<BestNodeReplication>(
					new Comparator<BestNodeReplication>() {
						public int compare(BestNodeReplication o1, BestNodeReplication o2) {
							int diff = o1.deathTime - o2.deathTime;
							if (diff == 0) {
								diff = o1.getNode().getIndex() - o2.getNode().getIndex();
							}
							return -diff;
						};
					});
		} else {
			candidates = new ArrayList<BestNodeReplication>();
		}
		BestNodeReplication current = this;
		for (int it = 0; it < BestNodeReplication.replication + 1; it++) {
			current = current.getProtocol(current.getSuccessor().n);
			candidates.add(current);
		}
		return candidates;
	}

	private Finger getSuccessor() {
		return this.successor;
	}

	private Finger getPredecessor() {
		return this.predecessor;
	}

	private Finger setSuccessor(Finger successor) {
		Finger val = this.successor;
		this.successor = successor;
		return val;
	}

	private Finger setPredecessor(Finger predecessor) {
		Finger val = this.predecessor;
		this.predecessor = predecessor;
		return val;
	}

	@Override
	public void startup(Node myNode, Collection<Pair<Node, Integer>> availabilityList,
			int deathTime1, int currentTime) {
		this.checkIntegrity();
		assert (myNode.getFailState() == Fallible.DOWN);
		myNode.setFailState(Fallible.OK);
		if (BestNodeReplication.error > 0) {
			double mean = deathTime1 - currentTime;
			double variance = BestNodeReplication.error;
			long val = Math.round(mean + CommonState.r.nextGaussian() * variance);
			if (val <= 0) {
				val = 1;
			}
			// System.out.println(val + " " + (int) (currentTime + val) + " " +
			// deathTime1);
			this.deathTime = (int) (currentTime + val);
		} else {
			this.deathTime = deathTime1;
		}
		this.triggerJoin(myNode, myNode);
		this.checkIntegrity();
	}

	@Override
	public void kill(Collection<Pair<Node, Integer>> availabilityList) {
		this.checkIntegrity();
		assert (this.getNode().getFailState() == Fallible.OK);
		this.killed();
		this.getNode().setFailState(Fallible.DOWN);
		this.checkIntegrity();
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
		BestNodeReplication.simStarted = true;
		if (this.isUp()) {
			TreeSet<Key> myKeys = new TreeSet<Key>();
			assert (this.getPredecessor() != null) : this.getPredecessor();
			assert (this.getPredecessor().id != null) : this.getPredecessor() + " "
					+ this.getPredecessor().id;
			myKeys.addAll(BestNodeReplication.keyCreator.getRangeTop(this.getPredecessor(),
					this.finger));
			myKeys.addAll(BestNodeReplication.keyCreator.getRangeBottom(this.getPredecessor(),
					this.finger));

			for (Key it : myKeys) {
				ComplexKey theKey = ((ComplexKey) it);
				this.myKeys.add(theKey);
				assert (theKey.ownerNode == null);
				theKey.ownerNode = this.getNode();
			}

			this.replicate(this.myKeys);
		}
		if (myNode.getIndex() == Network.size() - 1) {
			this.checkIntegrity();
		}
	}

	static BestNodeReplication getNSuccessor(BestNodeReplication start, int index) {
		assert (index > 0);
		assert (start.isUp());
		Finger currentNode = start.getSuccessor();
		// set on correct neighbour
		for (int it = 0; it < index - 1; it++) {
			assert (currentNode != start.finger);
			BestNodeReplication proto = start.getProtocol(currentNode.n);
			assert (proto.isUp());
			currentNode = proto.getSuccessor();
		}
		assert (currentNode != start.finger);
		BestNodeReplication toReturn = start.getProtocol(currentNode.n);
		assert (toReturn.isUp());
		return toReturn;
	}

	static BestNodeReplication getNPredecessor(BestNodeReplication start, int index) {
		assert (index > 0);
		assert (start.isUp());
		Finger currentNode = start.getPredecessor();
		// set on correct neighbour
		for (int it = 0; it < index - 1; it++) {
			assert (currentNode != start.finger);
			BestNodeReplication proto = start.getProtocol(currentNode.n);
			assert (proto.isUp());
			currentNode = proto.getPredecessor();
		}
		assert (currentNode != start.finger);
		BestNodeReplication toReturn = start.getProtocol(currentNode.n);
		assert (toReturn.isUp());
		return toReturn;
	}

	private void checkIntegrity() {
		if (!BestNodeReplication.simStarted)
			return;

		if (true)
			return;

		int keysSum = 0;
		int replicatedKeysSum = 0;
		for (int it = 0; it < Network.size(); it++) {
			BestNodeReplication proto = this.getProtocol(Network.get(it));
			if (!proto.isUp()) {
				assert (proto.myKeys == null || proto.myKeys.size() == 0) : proto;
				continue;
			}
			keysSum += proto.myKeys.size();
			replicatedKeysSum += proto.myReplicatedKeys.size();

			assert (proto.myKeys.size() == BestNodeReplication.keyCreator.getRangeSize(
					proto.getPredecessor(), proto.finger));
		}
		assert (keysSum == BestNodeReplication.keyCreator.getNKeys()) : keysSum + " "
				+ BestNodeReplication.keyCreator.getNKeys();
		assert (replicatedKeysSum == BestNodeReplication.keyCreator.getNKeys()
				* (BestNodeReplication.replication - 1)) : keysSum + " "
				+ BestNodeReplication.keyCreator.getNKeys() + " "
				+ (BestNodeReplication.replication - 1);
		return;
	}

	@Override
	public String toString() {
		return this.finger + " " + this.getSuccessor() + " " + this.getPredecessor() + " "
				+ this.myKeys.size() + " " + this.myReplicatedKeys.size() + " " + this.isUp();
	}

	@Override
	public int getKeys() {
		return this.myKeys.size() + this.myReplicatedKeys.size();
	}

	@Override
	public int getReplicationDegree() {
		return BestNodeReplication.replication;
	}

	public static TreeSet<Key> getAllKeys() {
		return BestNodeReplication.keyCreator.getAllKeys();
	}

	@Override
	public int getMonitoringCount() {
		HashSet<Node> set = new HashSet<Node>();
		for (ComplexKey it : this.myKeys) {
			assert (it.ownerNode == this.getNode());
			for (Node it2 : it.replicas) {
				assert (it2 != null);
				set.add(it2);
			}
		}
		return set.size();
	}

	@Override
	public int getAndClearChainMessages() {
		Collection<List<?>> chains = calculateChains();
		int retval = 0;
		for (List<?> i : chains) {
			retval += i.size();
		}
		return retval;
	}

	private HashSet<List<?>> calculateChains() {
		HashSet<List<?>> chains = new HashSet<List<?>>();
		for (ComplexKey i : myKeys) {
			chains.add(Arrays.asList(i.replicas));
		}
		return chains;
	}
}
