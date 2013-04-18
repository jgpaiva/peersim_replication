package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.Killable;
import gsd.jgpaiva.protocols.replication.BestNodeReplication2;
import gsd.jgpaiva.protocols.replication.GroupReplication2;
import gsd.jgpaiva.protocols.replication.NeighbourReplication;
import gsd.jgpaiva.structures.dht.Finger;
import gsd.jgpaiva.structures.replication.ComplexKey;
import gsd.jgpaiva.structures.replication.Key;
import gsd.jgpaiva.utils.IncrementalStats;
import gsd.jgpaiva.utils.Pair;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalFreq;

public class Queries2 extends ControlImpl {

	private static final String PAR_REPEATS = "repeats";
	private static final String PAR_DISTRIBUTION = "distribution";
	private static final String PAR_ALTERNATIVE = "alternative";
	private final int repeats;
	private final String distribution;
	private final BufferedReader br;
	private boolean alternativeMode;

	public Queries2(String prefix) {
		super(prefix);
		this.repeats = Configuration.getInt(prefix + "." + Queries2.PAR_REPEATS);
		if (Configuration.contains(prefix + "." + Queries2.PAR_DISTRIBUTION)) {
			this.distribution = Configuration.getString(prefix + "." + Queries2.PAR_DISTRIBUTION);
		} else {
			this.distribution = null;
		}
		if (Configuration.contains(prefix + "." + Queries2.PAR_ALTERNATIVE)) {
			this.alternativeMode = Configuration.getBoolean(prefix + "." + Queries2.PAR_ALTERNATIVE);
			throw new RuntimeException("What are you doing?");
		} else {
			this.alternativeMode = false;
		}
		if (this.distribution != null) {
			try {
				this.br = new BufferedReader(new FileReader(this.distribution));
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			this.br = null;
		}
	}

	@Override
	protected boolean executeCycle() {
		System.err.println("Starting Queries");
		IncrementalFreq loads = new IncrementalFreq();
		IncrementalStats loads2 = new IncrementalStats();
		IncrementalStats keyStats = new IncrementalStats();

		int[] nodeLoads = new int[Network.size()];
		int counter = 0;

		if (Network.get(0).getProtocol(this.pid) instanceof GroupReplication2) {
			HashSet<TreeSet<Node>> groups = GroupReplication2.getGroups();
			for (TreeSet<Node> group : groups) {
				ArrayList<Pair<Node, Integer>> nodes = new ArrayList<Pair<Node, Integer>>(
						group.size());
				for (Node it : group)
					nodes.add(new Pair<Node, Integer>(it, 0));

				int load = ((GroupReplication2) group.first().getProtocol(this.pid)).getLoad();
				keyStats.add(load, group.size());
				for (int r = 0; r < this.repeats; r++)
					for (int it = 0; it < load; it++) {
						Node n = addCostToRandomNode(nodes);
						nodeLoads[n.getIndex()]++;
					}

				printStatus(++counter, groups.size());
			}
			addLoads(loads, loads2, nodeLoads);
		} else if (Network.get(0).getProtocol(this.pid) instanceof BestNodeReplication2) {
			TreeSet<Key> keys = BestNodeReplication2.getAllKeys();
			for (Key it : keys) {
				int toRepeat = this.getNumQueries();
				ComplexKey key = (ComplexKey) it;
				for (int r = 0; r < toRepeat; r++) {
					int index = CommonState.r.nextInt(key.replicas.length + 1);
					int nodeIndex;
					if (index < key.replicas.length) {
						nodeIndex = key.replicas[index].getIndex();
					} else {
						nodeIndex = key.ownerNode.getIndex();
					}
					nodeLoads[nodeIndex]++;
				}
				printStatus(++counter, keys.size());
			}
			createStats(loads, loads2, keyStats, nodeLoads);
		} else if (Network.get(0).getProtocol(this.pid) instanceof NeighbourReplication) {
			int replication = ((NeighbourReplication) Network.get(0).getProtocol(this.pid))
					.getReplicationDegree();
			for (Finger node : NeighbourReplication.activeNodes) {
				NeighbourReplication proto = (NeighbourReplication) node.n.getProtocol(this.pid);
				ArrayList<NeighbourReplication> options = new ArrayList<NeighbourReplication>(
						replication + 1);
				options.add(proto);
				NeighbourReplication current = proto;
				for (int it = 0; it < replication; it++) {
					Finger temp = current.getPredecessor();
					current = (NeighbourReplication) temp.n.getProtocol(this.pid);
					options.add(current);
				}
				for (int it = 0; it < proto.getMyKeys(); it++) {
					int toRepeat = this.getNumQueries();
					for (int r = 0; r < toRepeat; r++) {
						int index = CommonState.r.nextInt(options.size());
						int nodeIndex = options.get(index).getNode().getIndex();
						nodeLoads[nodeIndex]++;
					}
				}
				printStatus(++counter, NeighbourReplication.activeNodes.size());
			}
			createStats(loads, loads2, keyStats, nodeLoads);
		}

		int totalNodes = getTotalNodes(nodeLoads);
		this.println("Total nodes:" + totalNodes);
		this.println("Percentage for 50 Percent load: "
				+ getPercentageFor50PercentLoad(nodeLoads, totalNodes));
		this.println("Percentage of nodes that reply to at least one query: "
				+ moreThanZeroQueries(nodeLoads, totalNodes));
		this.println(loads);
		this.println(loads2.toStringLong());
		this.println(keyStats.toStringLong());
		return false;
	}

	private int getTotalNodes(int[] nodeLoads) {
		int retval = 0;
		for (int i = 0; i < nodeLoads.length; i++) {
			Killable n = (Killable) Network.get(i).getProtocol(pid);
			if (!n.isUp()) {
				nodeLoads[i] = -1;
			} else {
				retval++;
			}
		}
		return retval;
	}

	private static double moreThanZeroQueries(int[] nodeLoads, int totalNodes) {
		int acc = 0;
		for (int it : nodeLoads) {
			if (it > 0)
				acc++;
		}
		return ((double) acc) / totalNodes;
	}

	private static double getPercentageFor50PercentLoad(int[] nodeLoads, int totalNodes) {
		int[] loadsCopy = new int[nodeLoads.length];
		System.arraycopy(nodeLoads, 0, loadsCopy, 0, nodeLoads.length);

		Arrays.sort(loadsCopy);

		int total = 0;
		for (int it : loadsCopy)
			total += it;

		int half = total / 2;
		int acc = 0;
		int counter = 0;
		for (int i = loadsCopy.length - 1; i >= 0; i--) {
			if (loadsCopy[i] < 0)
				continue;
			acc += loadsCopy[i];
			counter++;
			if (acc > half)
				break;
		}
		return ((double) counter) / totalNodes;
	}

	private void createStats(IncrementalFreq loads, IncrementalStats loads2, IncrementalStats keyStats,
			int[] nodeLoads) {
		int added = addLoads(loads, loads2, nodeLoads);
		for (int it = 0; it < Network.size(); it++) {
			KeyStorageProtocol proto = (KeyStorageProtocol) Network.get(it).getProtocol(
					this.pid);
			if (proto.isUp()) {
				added--;
				keyStats.add(proto.getKeys());
			} else {
				assert (proto.getKeys() == 0);
			}
		}
		if (added > 0) {
			loads.add(0, added);
			loads2.add(0, added);
		}
	}

	private static int addLoads(IncrementalFreq loads, IncrementalStats loads2, int[] nodeLoads) {
		int counter = 0;
		int added = 0;
		for (int it : nodeLoads) {
			if (it > 0) {
				loads.add(it);
				loads2.add(it);
				added++;
			}
			printStatus(++counter, nodeLoads.length);
		}
		return added;
	}

	private static void printStatus(int counter, int total) {
		if ((counter) % (total / 100) == 0)
			System.err.print("|");

		if ((counter) % (total / 10) == 0)
			System.err.println((counter) + "/" + total);
	}

	private static Node addCostToRandomNode(ArrayList<Pair<Node, Integer>> nodes) {
		int index = CommonState.r.nextInt(nodes.size());
		Pair<Node, Integer> toAdd = nodes.get(index);
		toAdd.snd++;
		return toAdd.fst;
	}

	private int getNumQueries() {
		if (this.br == null)
			return this.repeats;
		else {
			try {
				return Integer.valueOf(this.br.readLine());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}

// } ALTERNATIVEMODE?
// else if (this.alternativeMode) {
// int replication = ((NeighbourReplication)
// Network.get(0).getProtocol(this.pid))
// .getReplicationDegree();
// Finger[] activeNodes = NeighbourReplication.activeNodes.toArray(new
// Finger[0]);
// UniqueRandomSample urs = new UniqueRandomSample(activeNodes.length,
// CommonState.r);
// for (Finger node : NeighbourReplication.activeNodes) {
// NeighbourReplication proto = (NeighbourReplication)
// node.n.getProtocol(this.pid);
// for (int it = 0; it < proto.getMyKeys(); it++) {
// int toRepeat = this.getNumQueries();
// ArrayList<Node> options = new ArrayList<Node>(replication + 1);
// options.add(proto.getNode());
// urs.reset();
// for (int it2 = 0; it2 < replication; it2++) {
// options.add(Network.get(urs.next()));
// }
// for (int r = 0; r < toRepeat; r++) {
// int index = CommonState.r.nextInt(options.size());
// int nodeIndex = options.get(index).getIndex();
// nodeLoads[nodeIndex]++;
// }
// }
// printStatus(++counter, NeighbourReplication.activeNodes.size());
// }
// createStats(loads, loads2, keyStats, nodeLoads);