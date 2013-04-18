package gsd.jgpaiva.observers;

import gsd.jgpaiva.protocols.ProtocolStub;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.StringTokenizer;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Control;
import peersim.core.Node;

public class Debug implements Control {
	private static final String PAR_INIT_STEP = "initstep";
	private static final String PAR_FINAL_STEP = "finalstep";
	private static final String PAR_NODES = "nodes";

	private final int initStep;
	private final int finalStep;
	private int currentStep = 0;
	private final ArrayList<Long> nodes;
	private final ArrayList<PrintStream> psList;
	private boolean active = false;

	private static Debug instance = null;

	public static Debug getInstance() {
		return Debug.instance;
	}

	public Debug(String prefix) {
		this.initStep = Configuration
				.getInt(prefix + "." + Debug.PAR_INIT_STEP);
		if (Configuration.contains(prefix + "." + Debug.PAR_FINAL_STEP)) {
			this.finalStep = Configuration.getInt(prefix + "."
					+ Debug.PAR_FINAL_STEP);
		} else {
			this.finalStep = -1;
		}

		this.nodes = new ArrayList<Long>();
		if (Configuration.contains(prefix + "." + Debug.PAR_NODES)) {
			String temp = Configuration.getString(prefix + "."
					+ Debug.PAR_NODES);
			StringTokenizer tk = new StringTokenizer(temp, " ,;");
			while (tk.hasMoreElements()) {
				long value = Long.parseLong(tk.nextToken());
				this.nodes.add(value);
			}
		}

		Debug.instance = this;

		if (this.nodes.size() > 0) {
			String nodeList = "";
			for (Long it : this.nodes) {
				nodeList += it + " ";
			}
			System.err.println(prefix + " debugging on nodes " + nodeList);
		}
		this.psList = new ArrayList<PrintStream>();
		for (Long it : this.nodes) {
			try {
				this.psList.add(new PrintStream(("debug_node_" + it)));
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		}
	}

	public boolean contains(Node node) {
		return this.nodes.contains(node.getID());
	}

	@Override
	public boolean execute() {
		if (this.nodes.size() == 0) return false;

		this.currentStep++;
		if (this.currentStep < this.initStep) return false;
		if (!this.active) {
			this.active = true;
		}

		if (this.finalStep > 0 && this.currentStep > this.finalStep) {
			if (this.active) {
				this.active = false;
			}
			return false;
		}

		return false;
	}

//	public void debug(Scatter proto, String s) {
//		if (!this.active) return;
//		Node node = proto.getNode();
//		if (node != null) {
//			int nodeIndex = this.nodes.indexOf(node.getID());
//			if (nodeIndex < 0)
//				throw new RuntimeException("Should never happen");
//
//			String clusterID = proto.getID() != null ? proto.getID().toString(
//					Character.MAX_RADIX) : "null";
//			PrintStream ps = this.psList.get(nodeIndex);
//			ps.println(CommonState.getTime() + " " + node.getID() + " "
//					+ proto.getName() + " " + clusterID + " " + s);
//		}
//	}
//
//	public void debug(Rollerchain proto, String s) {
//		if (!this.active) return;
//		Node node = proto.getNode();
//		if (node != null) {
//			int nodeIndex = this.nodes.indexOf(node.getID());
//			if (nodeIndex < 0)
//				throw new RuntimeException("Should never happen");
//
//			String clusterID = proto.getID() != null ? proto.getID().toString(
//					Character.MAX_RADIX) : "null";
//			PrintStream ps = this.psList.get(nodeIndex);
//			ps.println(CommonState.getTime() + " " + node.getID() + " "
//					+ proto.getName() + " " + clusterID + " " + s);
//		}
//	}

	public void debug(ProtocolStub proto, String s) {
		if (!this.active) return;
		Node node = proto.getNode();
		if (node != null) {
			int nodeIndex = this.nodes.indexOf(node.getID());
			if (nodeIndex < 0)
				throw new RuntimeException("Should never happen");

			PrintStream ps = this.psList.get(nodeIndex);
			ps.println(CommonState.getTime() + " " + node.getID() + " "
					+ proto.getName() + " " + s);
		}
	}
}