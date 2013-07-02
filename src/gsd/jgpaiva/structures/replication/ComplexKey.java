package gsd.jgpaiva.structures.replication;

import java.math.BigInteger;

import peersim.core.Node;

public class ComplexKey extends Key {
	public Node ownerNode;
	public Node[] replicas;

	public ComplexKey(BigInteger value) {
		super(value);
	}

	public ComplexKey(BigInteger value, int load) {
		super(value, load);
	}
}
