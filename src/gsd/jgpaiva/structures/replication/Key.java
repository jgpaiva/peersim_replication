package gsd.jgpaiva.structures.replication;

import gsd.jgpaiva.utils.Identifier;

import java.math.BigInteger;

public class Key implements Comparable<Key> {

	public static Key[] allKeys;

	public static boolean multiKey;

	public final BigInteger value;
	public final MultiKey owner;
	public final int load;

	public Key(BigInteger value) {
		this.value = new Identifier(value);
		this.owner = null;
		load = 0;
	}

	public Key(BigInteger value, MultiKey owner) {
		this.value = new Identifier(value);
		this.owner = owner;
		load = 0;
	}

	public Key(BigInteger value, int load) {
		this.value = new Identifier(value);
		this.owner = null;
		this.load = load;
	}

	@Override
	public final boolean equals(Object obj) {
		return obj instanceof Key ? ((Key) obj).value.equals(this.value)
				: false;
	}

	@Override
	public final int compareTo(Key key) {
		return this.value.compareTo(key.value);
	}

	@Override
	public final String toString() {
		return this.value.toString(Character.MAX_RADIX);
	}

	@Override
	public final int hashCode() {
		if (this.value == null)
			return 0;
		return this.value.hashCode();
	}
}
