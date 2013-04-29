package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.interfaces.CostAwareMessage;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.protocols.ProtocolStub;
import gsd.jgpaiva.structures.dht.Finger;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.Identifier;
import gsd.jgpaiva.utils.KeyCreator;
import gsd.jgpaiva.utils.Pair;

import java.math.BigInteger;
import java.util.Collection;
import java.util.TreeSet;

import peersim.core.CommonState;
import peersim.core.Fallible;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.Protocol;

public class NeighbourReplication extends ProtocolStub implements Protocol,
		UptimeSimulatorProtocol, KeyStorageProtocol, MonitorableProtocol {
	public static final String PAR_REPL = "replication";

	private static int idLength;
	private static BigInteger ringSize;
	private static KeyCreator keyCreator = KeyCreator.getInstance();
	private static int replication;
	public final static TreeSet<Finger> activeNodes = new TreeSet<Finger>();

	static boolean simStarted = false;

	private Finger finger;
	private Finger successor;
	private Finger predecessor;
	private int keys = 0;
	private int replicatedKeys = 0;

	public NeighbourReplication(String prefix) {
		super(prefix);
		NeighbourReplication.idLength = GlobalConfig.getIdLength();
		NeighbourReplication.ringSize = BigInteger.ONE.add(BigInteger.ONE).pow(
				NeighbourReplication.idLength);
		NeighbourReplication.replication = GlobalConfig.getReplication();
	}

	@Override
	public NeighbourReplication clone() {
		NeighbourReplication c = (NeighbourReplication) super.clone();
		c = (NeighbourReplication) super.clone();
		return c;
	}

	@Override
	public boolean shouldPrint() {
		return true;
	}

	NeighbourReplication getProtocol(Node node) {
		NeighbourReplication val = (NeighbourReplication) node.getProtocol(ProtocolStub.pid);
		return val;
	}

	@Override
	public void join(Node contact) {
		this.finger = new Finger(this.getNode(), new Identifier(new BigInteger(
				NeighbourReplication.idLength, CommonState.r)));
		// step 1: get successor and set as predecessor
		if (NeighbourReplication.activeNodes.size() == 0) {
			this.setSuccessor(null);
			this.setPredecessor(null);
		} else {
			Finger successor = NeighbourReplication.activeNodes.ceiling(this.finger);
			if (successor == null) {
				successor = NeighbourReplication.activeNodes.first();
			}
			this.setSuccessor(successor);
			Finger oldPred = this.getProtocol(successor.n).setPredecessor(this.finger);
			if (oldPred == null) {
				Finger val = this.getProtocol(successor.n).setSuccessor(this.finger);
				assert (val == null) : this.finger + " " + this.successor + " " + this.predecessor
						+ " " + NeighbourReplication.activeNodes;
				this.setPredecessor(successor);
			} else {
				Finger val = this.getProtocol(oldPred.n).setSuccessor(this.finger);
				assert (val == this.getSuccessor());
				this.setPredecessor(oldPred);
			}
		}

		if (NeighbourReplication.simStarted) {
			assert (this.checkIntegrity());
			// step 2: get my keys
			if (this.getSuccessor() != null) {
				this.getProtocol(this.getSuccessor().n).getMyKeys(this);
			} else {
				// I get all keys
				this.keys = NeighbourReplication.keyCreator.getRangeSize(this.finger, this.finger);
			}

			// step 3 : get replicated keys
			if (this.getPredecessor() != null) {
				Finger currentNode = this.getPredecessor();
				for (int it = 0; it < NeighbourReplication.replication; it++) {
					assert (currentNode != this.finger);
					NeighbourReplication proto = this.getProtocol(currentNode.n);
					proto.getReplKeys(this);
					currentNode = proto.getPredecessor();
				}
			}
			assert (this.checkIntegrity());
		}

		NeighbourReplication.activeNodes.add(this.finger);
	}

	private void getMyKeys(NeighbourReplication other) {
		int oldKeys = this.keys;
		int newKeys = NeighbourReplication.keyCreator.getRangeSize(other.finger, this.finger);
		int toMove = oldKeys - newKeys;
		this.moveKeysFrom(this, other, toMove);
		NeighbourReplication proto = NeighbourReplication.getNSuccessor(this,
				NeighbourReplication.replication);
		this.moveReplKeysFrom(proto, this, toMove);
		this.addCost(new KeyTransferMessage(toMove));

		assert (other.finger == this.getPredecessor());
		assert (this.keys <= oldKeys);
		assert (toMove == NeighbourReplication.keyCreator.getRangeSize(other.getPredecessor(),
				other.finger));
	}

	private void getReplKeys(NeighbourReplication target) {
		assert (NeighbourReplication.keyCreator.getRangeSize(this.getPredecessor(), this.finger) == this.keys);
		this.addCost(new KeyTransferMessage(this.keys));
		NeighbourReplication proto = NeighbourReplication.getNSuccessor(this,
				NeighbourReplication.replication + 1);
		this.moveReplKeysFrom(proto, target, this.keys);
	}

	public void killed() {
		NeighbourReplication.activeNodes.remove(this.finger);
		this.finger = null;

		// step1: remove my node from topology
		if (this.getSuccessor() == this.getPredecessor()) {
			if (this.getSuccessor() == null) return;
			this.getProtocol(this.getSuccessor().n).setPredecessor(null);
			this.getProtocol(this.getSuccessor().n).setSuccessor(null);
		} else {
			this.getProtocol(this.getSuccessor().n).setPredecessor(this.getPredecessor());
			this.getProtocol(this.getPredecessor().n).setSuccessor(this.getSuccessor());
		}

		if (NeighbourReplication.simStarted) {
			assert (this.checkIntegrity());
			assert (this.getSuccessor() != null);
			// step2: add my data to my successor
			this.getProtocol(this.getSuccessor().n).takeMyKeys(this);

			// step3: re-replicate my data
			assert (this.getPredecessor() != null);

			Finger currentNode = this.getPredecessor();
			for (int it = 0; it < NeighbourReplication.replication; it++) {
				assert (currentNode != this.finger);
				NeighbourReplication proto = this.getProtocol(currentNode.n);
				proto.takeReplKeys(this);
				currentNode = proto.getPredecessor();
			}

			// set on correct neighbour
			NeighbourReplication proto = NeighbourReplication.getNSuccessor(this,
					NeighbourReplication.replication + 1);
			this.moveReplKeysFrom(this, proto, this.replicatedKeys);

			assert (this.keys == 0);
			assert (this.replicatedKeys == 0);
			assert (this.checkIntegrity());
		}
	}

	private void takeMyKeys(NeighbourReplication otherNode) {
		int movedKeys = otherNode.keys;
		this.moveKeysFrom(otherNode, this, movedKeys);
		NeighbourReplication proto = NeighbourReplication.getNSuccessor(this,
				NeighbourReplication.replication);
		this.moveReplKeysFrom(this, proto, movedKeys);
		this.addCost(new KeyTransferMessage(movedKeys));
		assert (this.keys == NeighbourReplication.keyCreator.getRangeSize(this.getPredecessor(),
				this.finger));
	}

	private void takeReplKeys(NeighbourReplication sender) {
		assert (NeighbourReplication.keyCreator.getRangeSize(this.getPredecessor(), this.finger) == this.keys);
		this.addCost(new KeyTransferMessage(this.keys));
		NeighbourReplication target = NeighbourReplication.getNSuccessor(this,
				NeighbourReplication.replication);
		this.moveReplKeysFrom(sender, target, this.keys);
	}

	private Finger getSuccessor() {
		return this.successor;
	}

	public Finger getPredecessor() {
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
			int deathTime, int currentTime) {
		assert (myNode.getFailState() == Fallible.DOWN);
		myNode.setFailState(Fallible.OK);
		this.triggerJoin(myNode, myNode);
	}

	@Override
	public void kill(Collection<Pair<Node, Integer>> availabilityList) {
		assert (this.getNode().getFailState() == Fallible.OK);
		this.killed();
		this.getNode().setFailState(Fallible.DOWN);
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
		NeighbourReplication.simStarted = true;
		if (this.isUp()) {
			this.keys = NeighbourReplication.keyCreator.getRangeSize(this.getPredecessor(),
					this.finger);

			Finger currentNode = this.getSuccessor();
			for (int it = 0; it < NeighbourReplication.replication; it++) {
				assert (currentNode != this.finger);
				NeighbourReplication proto = this.getProtocol(currentNode.n);
				assert (proto.isUp());
				proto.replicatedKeys += this.keys;
				currentNode = proto.getSuccessor();
			}
		}
		if (myNode.getIndex() == Network.size() - 1) {
			assert (this.checkIntegrity());
		}
	}

	private void moveKeysFrom(NeighbourReplication from, NeighbourReplication to, int numKeys) {
		assert (from != to);
		assert (from.isUp());
		assert (to.isUp());
		assert (from.keys >= numKeys);
		to.keys += numKeys;
		from.keys -= numKeys;
	}

	private void moveReplKeysFrom(NeighbourReplication from, NeighbourReplication to, int numKeys) {
		assert (from != to);
		assert (from.isUp());
		assert (to.isUp());
		assert (from.replicatedKeys >= numKeys);
		to.replicatedKeys += numKeys;
		from.replicatedKeys -= numKeys;
	}

	static NeighbourReplication getNSuccessor(NeighbourReplication start, int index) {
		assert (index > 0);
		assert (start.isUp());
		Finger currentNode = start.getSuccessor();
		// set on correct neighbour
		for (int it = 0; it < index - 1; it++) {
			assert (currentNode != start.finger);
			NeighbourReplication proto = start.getProtocol(currentNode.n);
			assert (proto.isUp());
			currentNode = proto.getSuccessor();
		}
		assert (currentNode != start.finger);
		NeighbourReplication toReturn = start.getProtocol(currentNode.n);
		assert (toReturn.isUp());
		return toReturn;
	}

	private boolean checkIntegrity() {
		if (!NeighbourReplication.simStarted) return true;

		if (true) return true;

		int keysSum = 0;
		int replicationSum = 0;
		for (int it = 0; it < Network.size(); it++) {
			NeighbourReplication proto = this.getProtocol(Network.get(it));
			if (!proto.isUp()) {
				assert (proto.keys == 0) : proto;
				assert (proto.replicatedKeys == 0) : proto;
				continue;
			}
			keysSum += proto.keys;
			replicationSum += proto.replicatedKeys;
		}
		assert (keysSum == NeighbourReplication.keyCreator.getNKeys()) : keysSum + " "
				+ NeighbourReplication.keyCreator.getNKeys();
		assert (replicationSum == keysSum * NeighbourReplication.replication) : keysSum + " "
				+ replicationSum + " " + NeighbourReplication.replication;
		return true;
	}

	@Override
	public String toString() {
		return this.finger + " " + this.getSuccessor() + " " + this.getPredecessor() + " "
				+ this.keys + " " + this.replicatedKeys + " " + this.isUp();
	}

	public int getMyKeys() {
		return this.keys;
	}

	@Override
	public int getKeys() {
		return this.keys + this.replicatedKeys;
	}

	@Override
	public int getReplicationDegree() {
		return NeighbourReplication.replication;
	}

	@Override
	public int getMonitoringCount() {
		return NeighbourReplication.replication;
	}

	public Finger getFinger() {
		return this.finger;
	}
}
