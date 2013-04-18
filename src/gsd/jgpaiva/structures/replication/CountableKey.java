package gsd.jgpaiva.structures.replication;

import java.math.BigInteger;

public class CountableKey extends Key {
	public int counter = 0;

	public CountableKey(BigInteger value) {
		super(value);
	}

	public CountableKey(BigInteger value, MultiKey owner) {
		super(value, owner);
	}
}
