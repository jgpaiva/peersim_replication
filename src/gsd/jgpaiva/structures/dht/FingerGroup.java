package gsd.jgpaiva.structures.dht;

import gsd.jgpaiva.utils.Identifier;

import java.math.BigInteger;
import java.util.Iterator;
import java.util.TreeSet;

import peersim.core.Node;
//import testing.TestNode;

public class FingerGroup extends TreeSet<Node> {
	private static final long serialVersionUID = 2048406186161564223L;
	private Identifier id;

	public FingerGroup(TreeSet<Node> nodes, Identifier id) {
		super(nodes);
		this.id = id;
	}

	@Override
	public FingerGroup clone() {
		FingerGroup cl = null;
		cl = (FingerGroup) super.clone();
		return cl;
	}

	@Override
	public String toString() {
		String nodes = "[ ";
		Iterator<Node> iterator = super.iterator();
		while (iterator.hasNext()) {
			nodes += iterator.next().getID() + " ";
		}
		nodes += "]";
		return "FG " + this.id + ":" + nodes;
	}

	/*public static void main(String[] args) {
		TreeSet<Node> testNodes = new TreeSet<Node>();
		for (int it = 0; it < 20; it++) {
			Node temp = new TestNode();
			temp.setIndex(it);
			testNodes.add(temp);
		}
		for (Node it : testNodes) {
			System.out.println(it);
		}

		TreeSet<Node> nl1 = new TreeSet<Node>();
		nl1.add(testNodes.first());
		FingerGroup fg1 = new FingerGroup(nl1, new Identifier(BigInteger.ZERO));
		System.out.println(fg1);
		FingerGroup fg2 = fg1.clone();
		fg1.add(testNodes.last());
		System.out.println(fg1);
		System.out.println(fg2);
	}*/

	public Identifier getID() {
		return this.id;
	}

}