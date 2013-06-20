package gsd.jgpaiva.controllers;

import gsd.jgpaiva.interfaces.UptimeSimulatorProtocol;
import gsd.jgpaiva.utils.GlobalConfig;
import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.UniqueRandomSample;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.Fallible;
import peersim.core.Network;
import peersim.core.Node;
import peersim.util.IncrementalFreq;

public class MembershipController extends ControlImpl {
	private static final String PAR_IN_FILE = "infile";
	private static final String PAR_MAX_NODES = "maxnodes";
	private static final String PAR_MAX_TIME = "maxtime";
	private static final String PAR_MIN_TIME = "mintime";
	private static final String PAR_SEED = "seed";

	private final String infile;

	private final FileInputStream fis;

	private final int maxNodes;
	private final int totalNodes = 8325045;

	private final int maxTime;
	private final int minTime;
	private int upNodes;

	private NodeHeader[] headers;
	private Pointers[] times;
	private int seed;
	private TreeSet<Pair<Node, Integer>> availabilityList = new TreeSet<Pair<Node, Integer>>(
			new Comparator<Pair<Node, Integer>>() {
				@Override
				public int compare(Pair<Node, Integer> o1, Pair<Node, Integer> o2) {
					int val = o1.snd - o2.snd;
					return (int) -(val == 0 ? o1.fst.getID() - o2.fst.getID() : val);
				}
			});
	private IncrementalFreq aliveTimeStats = new IncrementalFreq();
	private IncrementalFreq sessionTimeStats = new IncrementalFreq();
	private int replication = GlobalConfig.getReplication();

	public MembershipController(String prefix) {
		super(prefix);

		if (Configuration.contains(prefix + "." + MembershipController.PAR_IN_FILE)) {
			this.infile = Configuration.getString(prefix + "." + MembershipController.PAR_IN_FILE);
		} else {
			this.infile = "arguments/trace.bin";
		}
		this.maxNodes = Configuration.getInt(prefix + "." + MembershipController.PAR_MAX_NODES);
		this.maxTime = Configuration.getInt(prefix + "." + MembershipController.PAR_MAX_TIME);
		this.minTime = Configuration.getInt(prefix + "." + MembershipController.PAR_MIN_TIME);
		this.seed = Configuration.getInt(prefix + "." + MembershipController.PAR_SEED);

		if (this.getFinalStep() < 0)
			throw new IllegalArgumentException("finalstep should be > 0. recomended: 2196717");

		try {
			this.fis = new FileInputStream(this.infile);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e); // should never happen
		}

		this.initialize();

		IncrementalFreq stats = new IncrementalFreq();
		for (Pointers it : this.times) {
			if (it == null) {
				stats.add(0);
			} else {
				stats.add(it.max);
			}
		}
		this.println("times sizes:" + stats.toString());
		this.println("time slots sized:" + this.aliveTimeStats.toString());
		this.println("session times: " + this.sessionTimeStats.toString());
		this.aliveTimeStats = null;
		this.sessionTimeStats = null;
	}

	private void initialize() {
		System.err.print(this.name + ": selecting " + this.maxNodes);
		UniqueRandomSample r = new UniqueRandomSample(this.totalNodes, new Random(this.seed));
		System.err.print(" .");
		boolean[] nodes = new boolean[this.totalNodes];
		System.err.print(".");
		int inserted = 0;
		while (inserted++ < this.maxNodes) {
			nodes[r.next()] = true;
		}
		r = null;

		System.err.println(". done!");
		System.err.print(this.name + ": allocating structures .");
		this.headers = new NodeHeader[this.maxNodes];
		System.err.print(".");
		this.times = new Pointers[this.getFinalStep() + 1];
		System.err.println(". done!");

		try {
			int toRead = (4 + 4 + 4) * 227 * 2;
			ByteBuffer buffer = ByteBuffer.allocate(toRead);
			int oldNode = -1;
			int nodeCounter = -1;
			int nodeIndex = 0;
			ArrayList<Uptime> acc = new ArrayList<Uptime>(100);
			outer: while ((this.fis.getChannel().read(buffer)) > 0) {
				buffer.clear();
				while (buffer.hasRemaining()) {
					int node = buffer.getInt();
					assert (node != 0);
					int start = buffer.getInt();
					assert (start >= 0);
					int sessionTime = buffer.getInt();
					assert (sessionTime > 0);
					if (oldNode == -1) {
						oldNode = node;
					}
					if (oldNode != node) {
						nodeCounter++;
						if (nodes[nodeCounter]) {
							this.addAccum(nodeIndex, acc);
							nodeIndex++;

							if ((nodeIndex) % (this.maxNodes / 1000) == 0) {
								System.err.print("|");
							}
							if ((nodeIndex) % (this.maxNodes / 10) == 0) {
								System.err.println((nodeIndex) + "/" + this.maxNodes);
							}
						} else {
							acc.clear();
						}
					}
					if (nodeIndex >= this.maxNodes) {
						nodeCounter++;
						break outer;
					}
					if (sessionTime != 0) {
						acc.add(new Uptime(start, sessionTime));
					}
					oldNode = node;
				}
				buffer.clear();
			}
			if (nodeCounter < this.totalNodes && nodes[nodeCounter]) {
				this.addAccum(nodeIndex, acc);
				nodeIndex++;
			}
			assert (nodeIndex == this.maxNodes);
			assert (this.doCheck());
		} catch (IOException e) {
			throw new RuntimeException(e); // should never happen
		}

		for (int it = 0; it < Network.size(); it++) {
			Network.get(it).setFailState(Fallible.DOWN);
		}
	}

	private boolean doCheck() {
		int counter = 0;
		for (NodeHeader it : this.headers) {
			if (it != null) {
				for (int it2 : it.ends) {
					if (it2 < this.times.length
							&& !this.arrayContains(this.times[it2], counter)) {
						this.DIE(counter + " " + it2 + " "
								+ this.printArray(this.times[it2].p));
					}
				}
			}
			counter++;
		}
		return true;
	}

	private void setTime(int node, int time) {
		Pointers p = this.times[time];
		int[] is;
		if (p == null) {
			p = new Pointers();
			p.p = new int[10];
			this.times[time] = p;
		}
		is = p.p;
		if (p.max >= p.p.length) {
			is = Arrays.copyOf(is, is.length * 2);
			this.times[time].p = is;
		}
		is[p.max] = node;
		p.max++;
	}

	private void addAccum(int index, ArrayList<Uptime> acc) {
		List<Uptime> toRemove = new ArrayList<Uptime>();
		int lastEnd = -1;
		for (Uptime it : acc) {
			if (it.start == lastEnd) {
				it.start++; // discretize
			}
			if (it.start >= it.end) {
				toRemove.add(it);
			} else if (it.start < this.getFinalStep()) {
				this.setTime(index, it.start);
				if (it.end <= this.getFinalStep()) {
					this.setTime(index, it.end);
					lastEnd = it.end;
				}
			} else {
				toRemove.add(it);
			}
		}
		acc.removeAll(toRemove);

		if (acc.size() > 0) {
			int minConn = acc.get(0).start;
			int maxConn = acc.get(acc.size() - 1).end;
			this.aliveTimeStats.add((maxConn - minConn) / 100);
			for (Uptime it : acc) {
				this.sessionTimeStats.add((it.end - it.start) / 10);
			}

			this.headers[index] = new NodeHeader(acc);
		}
		acc.clear();
	}

	@Override
	protected boolean executeCycle() {
		int step = this.getStep();
		Pointers is = this.times[step];

		if (this.getStep() == this.minTime) {
			for (int it = 0; it < Network.size(); it++) {
				Node n = Network.get(it);
				UptimeSimulatorProtocol proto = (UptimeSimulatorProtocol) n.getProtocol(this.pid);
				proto.startSim(n);
			}
		}

		if (this.getStep() >= this.minTime && this.getStep() % 100 == 0) {
			this.println(step + " " + this.upNodes + " " + this.availabilityList);
		}

		if (is == null) return false;

		for (int it = 0; it < is.max; it++) {
			int node = is.p[it];
			NodeHeader header = this.headers[node];
			int currentEnd = header.currentPointer == -1 ? -1 : header.ends[header.currentPointer];
			if (currentEnd == step) {
				// is end
				this.kill(node);
				// cleanup
				if (header.currentPointer == header.ends.length) {
					this.headers[node] = null;
				}
			} else {
				// is start
				assert (step > currentEnd);
				header.currentPointer++;
				currentEnd = header.ends[header.currentPointer];
				this.startup(node, currentEnd, step);
				this.updateBestAvailabilityNode(node, currentEnd);
			}
		}
		// cleanup
		this.times[step] = null;
		return false;
	}

	private void updateBestAvailabilityNode(int nodeIndex, int end) {
		this.availabilityList.add(new Pair<Node, Integer>(Network.get(nodeIndex), end));
		while (this.availabilityList.size() > this.replication) {
			this.availabilityList.pollLast();
		}
	}

	private void startup(int node, int deathTime, int currentTime) {
		Node n = Network.get(node);
		UptimeSimulatorProtocol proto = (UptimeSimulatorProtocol) n.getProtocol(this.pid);
		proto.startup(n, this.availabilityList, deathTime, currentTime);
		this.upNodes++;
		assert (this.upNodes > 0);
	}

	private void kill(int node) {
		UptimeSimulatorProtocol proto = (UptimeSimulatorProtocol) Network.get(node).getProtocol(
				this.pid);
		proto.kill(this.availabilityList);
		this.upNodes--;
		assert (this.upNodes >= 0);
	}

	private static final class NodeHeader {
		public final int[] ends;
		public short currentPointer;

		NodeHeader(ArrayList<Uptime> acc) {
			this.currentPointer = -1;
			this.ends = new int[acc.size()];
			for (int it = 0; it < acc.size(); it++) {
				this.ends[it] = acc.get(it).end;
			}
		}

		@Override
		public String toString() {
			String r = "P" + this.currentPointer + " ";
			if (this.ends != null) {
				for (int it : this.ends) {
					r += it + " ";
				}
			} else {
				r += "null";
			}
			return r;
		}
	}

	private static final class Uptime {
		public int start;
		public int end;

		Uptime(int start, int sessionTime) {
			this.start = start;
			this.end = start + sessionTime;
		}

		@Override
		public String toString() {
			return "(" + this.start + "," + this.end + ")";
		}
	}

	private void DIE(String string) {
		throw new RuntimeException(string);
	}

	private static class Pointers {
		public Pointers() {
			this.max = 0;
			this.p = null;
		}

		int max;
		int[] p;
	}

	private String printArray(int[] is) {
		if (is == null) return "null";
		String r = "";
		for (int it : is) {
			r += it + ",";
		}
		return r;
	}

	private boolean arrayContains(Pointers times, int i) {
		for (int it = 0; it < times.max; it++) {
			if (times.p[it] == i) return true;
		}
		return false;
	}
}
