package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.Killable;
import gsd.jgpaiva.interfaces.LoadAwareProtocol;
import gsd.jgpaiva.protocols.replication.BestNodeReplication;
import gsd.jgpaiva.protocols.replication.GroupReplication;
import gsd.jgpaiva.protocols.replication.NeighbourReplication;
import gsd.jgpaiva.structures.dht.Finger;
import gsd.jgpaiva.structures.replication.ComplexKey;
import gsd.jgpaiva.structures.replication.Key;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.Identifier;
import gsd.jgpaiva.utils.IncrementalStats;
import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.Utils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalFreq;

public class Queries extends ControlImpl {

	private static final String PAR_REPEATS = "repeats";
	private static final String PAR_NUM_VSERVERS = "numvservers";
	private final int repeats;
	private final int numVservers;

	public Queries(String prefix) {
		super(prefix);
		this.repeats = Configuration.getInt(prefix + "." + Queries.PAR_REPEATS);
		if (Configuration.contains(prefix + "." + Queries.PAR_NUM_VSERVERS))
			this.numVservers = Configuration.getInt(prefix + "." + Queries.PAR_NUM_VSERVERS);
		else
			this.numVservers = -1;
	}

	@Override
	protected boolean executeCycle() {
		System.err.println("Starting Queries. Node is:"
				+ Network.get(0).getProtocol(pid).getClass().getName() + " vservers is:" + numVservers);

		int[] nodeLoads = new int[Network.size()];
		Set<Node>[] monitoring = initMonitoring();

		if (Network.get(0).getProtocol(this.pid) instanceof GroupReplication) {
			int[] nodeRealLoad = new int[Network.size()];
			queryGroupReplication(nodeLoads, nodeRealLoad, monitoring);
			genLoadsAndPrint(nodeRealLoad, "load:");
			genRealLoadAndPrint("realLoad:");

		} else if (Network.get(0).getProtocol(this.pid) instanceof BestNodeReplication) {
			queryBestNodeReplication(nodeLoads, monitoring);

		} else if (Network.get(0).getProtocol(this.pid) instanceof NeighbourReplication) {
			queryNeighbourReplication(nodeLoads, monitoring);
		}
		genLoadsAndPrint(nodeLoads, "keyLoad:");
		genKeysAndPrint("keys:");
		genMonitoringAndPrint(monitoring, "mon:");

		// deal with virtual nodes
		if (Network.get(0).getProtocol(pid) instanceof NeighbourReplication && numVservers > 0) {
			nodeLoads = new int[Network.size()];
			monitoring = initMonitoring();
			queryVirtualNodes(nodeLoads, monitoring);
			genLoadsAndPrint(nodeLoads, "vnode_keyLoad:");
			genKeysAndPrint("vnode_keys:");
			genMonitoringAndPrint(monitoring, "vnode_mon:");
		}
		return false;
	}

	private void queryVirtualNodes(int[] nodeLoads, Set<Node>[] monitoring) {
		int replication = ((NeighbourReplication) Network.get(0).getProtocol(this.pid))
				.getReplicationDegree();

		Pair<Finger, Integer>[] vnodes = createVnodes(NeighbourReplication.activeNodes, pid, numVservers);

		int counter = 0;
		for (int vnodeIndex = 0; vnodeIndex < vnodes.length; vnodeIndex++) {// all
																			// vnodes
			Pair<Finger, Integer> currPair = vnodes[vnodeIndex];
			Node node = currPair.fst.n;
			int keys = currPair.snd;
			List<Node> options = getOptionsArray(replication, vnodes, vnodeIndex);

			for (Node it : options) {
				if (it != node) {
					incrMonitoring(monitoring, node, it);
				}
			}
			for (int it = 0; it < keys; it++) { // all keys
				incrLoadRandomNode(nodeLoads, options);
			}
			printStatus(++counter, vnodes.length);
		}
	}

	private void queryNeighbourReplication(final int[] nodeLoads, final Set<Node>[] monitoring) {
		int counter = 0;
		int replication = ((NeighbourReplication) Network.get(0).getProtocol(this.pid))
				.getReplicationDegree();
		for (Finger f : NeighbourReplication.activeNodes) {
			ArrayList<Node> options = getNeighbourOptions(replication, f.n);

			for (Node it : options)
				incrMonitoring(monitoring, f.n, it);

			options.add(f.n);
			for (int it = 0; it < ((NeighbourReplication) f.n.getProtocol(this.pid)).getMyKeys(); it++) {
				incrLoadRandomNode(nodeLoads, options);
			}
			printStatus(++counter, NeighbourReplication.activeNodes.size());
		}
	}

	private void queryBestNodeReplication(int[] nodeLoads, Set<Node>[] monitoring) {
		int counter = 0;
		TreeSet<Key> keys = BestNodeReplication.getAllKeys();
		for (Key it : keys) {
			ComplexKey key = (ComplexKey) it;
			for (Node r : key.replicas) {
				incrMonitoring(monitoring, key.ownerNode, r);
			}

			ArrayList<Node> options = new ArrayList<Node>(key.replicas.length + 1);
			for (Node n : key.replicas) {
				options.add(n);
			}
			options.add(key.ownerNode);

			incrLoadRandomNode(nodeLoads, options);

			printStatus(++counter, keys.size());
		}
	}

	private void queryGroupReplication(int[] nodeKeys, int[] nodeLoad, Set<Node>[] monitoring) {
		int counter = 0;
		HashSet<TreeSet<Node>> groups = GroupReplication.getGroups();
		for (TreeSet<Node> group : groups) {
			for (Node n1 : group)
				// monitoring all-to-all
				for (Node n2 : group)
					if (n1 != n2)
						incrMonitoring(monitoring, n1, n2);

			int keys = ((KeyStorageProtocol) group.first().getProtocol(this.pid)).getKeys();
			for (int it = 0; it < keys; it++) {
				incrLoadRandomNode(nodeKeys, group);
			}

			int load = ((LoadAwareProtocol) group.first().getProtocol(this.pid)).getLoad();
			for (int it = 0; it < load; it++) {
				incrLoadRandomNode(nodeLoad, group);
			}

			printStatus(++counter, groups.size());
		}
	}

	// *****************************************************************
	// ************************ AUX FUNCTIONS **************************
	// *****************************************************************

	private void incrLoadRandomNode(int[] nodeLoads, Collection<Node> options) {
		for (int r = 0; r < this.repeats; r++) {
			Node el = Utils.getRandomEl(options);
			nodeLoads[el.getIndex()]++;
		}
	}

	private void incrMonitoring(Set<Node>[] monitoring, Node fromNode, Node toNode) {
		monitoring[fromNode.getIndex()].add(toNode);
	}

	private ArrayList<Node> getNeighbourOptions(int replication, Node n) {
		NeighbourReplication proto = (NeighbourReplication) n.getProtocol(this.pid);
		ArrayList<Node> options = new ArrayList<Node>(replication + 1);
		NeighbourReplication current = proto;
		for (int it = 0; it < replication; it++) {
			Finger temp = current.getPredecessor();
			current = (NeighbourReplication) temp.n.getProtocol(this.pid);
			options.add(current.getNode());
		}
		return options;
	}

	// *****************************************************************
	// ************************ STATS FUNCTIONS **************************
	// *****************************************************************

	private void genLoadsAndPrint(int[] nodeLoads, String prefix) {
		IncrementalFreq loadFreqs = new IncrementalFreq();
		IncrementalStats loadStats = new IncrementalStats();

		// finalize loads array
		setLoadsToNegative(nodeLoads);

		addLoads(loadFreqs, loadStats, nodeLoads);
		int totalNodes = getTotalNodes(nodeLoads);
		double fiftypercentload = getPercentageFor50PercentLoad(nodeLoads, totalNodes);
		double moreThanZero = getMoreThanZeroQueries(nodeLoads, totalNodes);

		printLoads(loadFreqs, loadStats, totalNodes, fiftypercentload, moreThanZero, prefix);
	}

	private void genKeysAndPrint(String prefix) {
		IncrementalFreq keyFreqs = new IncrementalFreq();
		IncrementalStats keyStats = new IncrementalStats();

		addKeys(keyFreqs, keyStats, pid);

		printFreqsAndStats(keyFreqs, keyStats, prefix);
	}

	private void genRealLoadAndPrint(String prefix) {
		IncrementalFreq loadFreqs = new IncrementalFreq();
		IncrementalStats loadStats = new IncrementalStats();

		addRealLoad(loadFreqs, loadStats, pid);

		printFreqsAndStats(loadFreqs, loadStats, prefix);
	}

	private void genMonitoringAndPrint(Set<Node>[] monitoring, String prefix) {
		IncrementalFreq monitorFreqs = new IncrementalFreq();
		IncrementalStats monitorStats = new IncrementalStats();

		addMonitoring(monitoring, monitorFreqs, monitorStats);

		printFreqsAndStats(monitorFreqs, monitorStats, prefix);
	}

	private void addMonitoring(Set<Node>[] monitoring, IncrementalFreq monitorFreqs,
			IncrementalStats monitorStats) {
		for (Set<Node> it : monitoring) {
			if (it == null)
				continue;
			monitorFreqs.add(it.size());
			monitorStats.add(it.size());
		}
	}

	private void setLoadsToNegative(int[] nodeLoads) {
		for (int i = 0; i < nodeLoads.length; i++) {
			Killable n = (Killable) Network.get(i).getProtocol(pid);
			if (!n.isUp()) {
				nodeLoads[i] = -1;
			}
		}
	}

	private static void addLoads(IncrementalFreq loadFreqs, IncrementalStats loadStats, int[] nodeLoads) {
		for (int it : nodeLoads) {
			if (it >= 0) {
				loadFreqs.add(it);
				loadStats.add(it);
			}
		}
	}

	private static void addRealLoad(IncrementalFreq freqs, IncrementalStats stats, int pid) {
		for (int it = 0; it < Network.size(); it++) {
			LoadAwareProtocol proto = (LoadAwareProtocol) Network.get(it).getProtocol(pid);
			int load = proto.getLoad();
			if (proto.isUp()) {
				freqs.add(load);
				stats.add(load);
			} else {
				assert (load == 0);
			}
		}
	}

	private static void addKeys(IncrementalFreq keyFreqs, IncrementalStats keyStats, int pid) {
		for (int it = 0; it < Network.size(); it++) {
			KeyStorageProtocol proto = (KeyStorageProtocol) Network.get(it).getProtocol(pid);
			int keys = proto.getKeys();
			if (proto.isUp()) {
				keyStats.add(keys);
				keyFreqs.add(keys);
			} else {
				assert (keys == 0);
			}
		}
	}

	private static double getPercentageFor50PercentLoad(int[] nodeLoads, int totalNodes) {
		int[] nodeLoadsCopy = new int[nodeLoads.length];
		System.arraycopy(nodeLoads, 0, nodeLoadsCopy, 0, nodeLoads.length);

		Arrays.sort(nodeLoadsCopy);

		int total = getLoadSum(nodeLoadsCopy);

		int halfLoad = total / 2;
		int acc = 0;
		int counter = 0;
		for (int i = nodeLoadsCopy.length - 1; i >= 0; i--) {
			if (nodeLoadsCopy[i] < 0) // ignore inactive nodes
				continue;
			acc += nodeLoadsCopy[i];
			counter++;
			if (acc > halfLoad)
				break;
		}
		return ((double) counter) / totalNodes;
	}

	private static double getMoreThanZeroQueries(int[] nodeLoads, int totalNodes) {
		int acc = 0;
		for (int it : nodeLoads) {
			if (it > 0)
				acc++;
		}
		return ((double) acc) / totalNodes;
	}

	private static int getLoadSum(int[] nodeLoads) {
		int total = 0;
		for (int it : nodeLoads) {
			if (it > 0) // ignore inactive nodes
				total += it;
		}
		return total;
	}

	private int getTotalNodes(int[] nodeLoads) {
		int acc = 0;
		for (int it : nodeLoads)
			if (it >= 0)
				acc++;
		return acc;
	}

	// *****************************************************************
	// ************************ VNODES FUNCTIONS ***********************
	// *****************************************************************

	private static Pair<Finger, Integer>[] createVnodes(final TreeSet<Finger> nodes, final int pid,
			final int numVservers) {
		TreeSet<Finger> temp = new TreeSet<Finger>();
		for (Finger node : nodes) {
			NeighbourReplication proto = (NeighbourReplication) node.n.getProtocol(pid);
			Finger f = proto.getFinger();
			temp.add(f);
			for (int it = 1; it < numVservers; it++) {
				// ignore one: it's the original one
				Finger nf = new Finger(f.n, new Identifier(new BigInteger(GlobalConfig.getIdLength(),
						CommonState.r)), true);
				temp.add(nf);
			}
		}
		Pair<Finger, Integer>[] sortedNodes = new Pair[temp.size()];
		int counter = 0;
		for (Finger it : temp)
			sortedNodes[counter++] = new Pair<Finger, Integer>(it, 0);

		for (int i = 0; i < sortedNodes.length; i++) {
			Pair<Finger, Integer> n = sortedNodes[i];
			if (!n.fst.isFake()) {
				randomizeKeysByVnodes(sortedNodes, i, pid);
			}
		}
		return sortedNodes;
	}

	private static void randomizeKeysByVnodes(final Pair<Finger, Integer>[] sortedNodes, final int i,
			final int pid) {
		ArrayList<Pair<Finger, Integer>> options = new ArrayList<Pair<Finger, Integer>>();
		Pair<Finger, Integer> realN = sortedNodes[i];
		options.add(realN);
		assert (sortedNodes[i].snd == 0);

		int current = (i - 1 + sortedNodes.length) % sortedNodes.length;
		while (sortedNodes[current].fst.isFake()) {
			options.add(sortedNodes[current]);
			assert (sortedNodes[current].snd == 0);
			current = (current - 1 + sortedNodes.length) % sortedNodes.length;
		}
		int keys = ((NeighbourReplication) realN.fst.n.getProtocol(pid)).getMyKeys();
		for (int it = 0; it < keys; it++) {
			Pair<Finger, Integer> r = Utils.getRandomEl(options);
			r.snd++;
		}
	}

	private List<Node> getOptionsArray(int replication, Pair<Finger, Integer>[] vnodes, int currentIndex) {
		Set<Node> options = new HashSet<Node>();
		// FIXME is the +1 correct?
		for (int it = currentIndex; options.size() < replication + 1; it = (it - 1 + vnodes.length)
				% vnodes.length) {
			options.add(vnodes[it].fst.n);
		}
		Node options_array[] = options.toArray(new Node[0]);
		return Arrays.asList(options_array);
	}

	private Set<Node>[] initMonitoring() {
		Set<Node>[] monitoring = new Set[Network.size()];
		for (int i = 0; i < monitoring.length; i++) {
			if (((Killable) Network.get(i).getProtocol(this.pid)).isUp())
				monitoring[i] = new HashSet<Node>();
		}
		return monitoring;
	}

	// *****************************************************************
	// ************************ PRINT FUNCTIONS ************************
	// *****************************************************************

	private static void printStatus(int counter, int total) {
		if ((counter) % (total / 100) == 0)
			System.err.print("|");

		if ((counter) % (total / 10) == 0)
			System.err.println((counter) + "/" + total);
	}

	private void printLoads(IncrementalFreq loadFreqs, IncrementalStats loadStats, int totalNodes,
			double fiftypercentload, double moreThanZero, String prefix) {
		this.println(prefix + "Total nodes: " + totalNodes);
		this.println(prefix + "Percentage for 50 Percent: " + fiftypercentload);
		this.println(prefix + "Percentage of nodes that have at least one: " + moreThanZero);
		this.printFreqsAndStats(loadFreqs, loadStats, prefix);
	}

	private void printFreqsAndStats(IncrementalFreq keyFreqs, IncrementalStats keyStats, String prefix) {
		this.println(prefix + "freqs: " + keyFreqs);
		this.println(prefix + "stats: " + keyStats.toStringLong());
	}
}