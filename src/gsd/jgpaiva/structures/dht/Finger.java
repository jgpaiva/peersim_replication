package gsd.jgpaiva.structures.dht;

import java.math.BigInteger;

import peersim.core.Node;

public class Finger implements Comparable<Finger> {
	public final Node n;
	public final BigInteger id;

	public Finger(Node n, BigInteger id) {
		if (n == null)
			throw new RuntimeException("Finger with a null node?");
		this.n = n;
		this.id = id;
	}

	public Finger(BigInteger id) {
		this.n = null;
		this.id = id;
	}

	@Override
	public final boolean equals(Object obj) {
		return obj instanceof Finger
				&& this.n.equals(((Finger) obj).n)
				&& this.id.equals(((Finger) obj).id);
	}

	@Override
	public final String toString() {
		return this.n + " " + this.id.toString(Character.MAX_RADIX);
	}

	@Override
	public final int compareTo(Finger o) {
		return this.id.compareTo(o.id);
	}

	@Override
	public final int hashCode() {
		if (this.id != null)
			return this.id.hashCode();
		return 0;
	}
}
