package gsd.jgpaiva.structures.replication;

import gsd.jgpaiva.utils.Identifier;

import java.math.BigInteger;

public class Key implements Comparable<Key> {

	public static Key[] allKeys;

	public static boolean multiKey;

	public final BigInteger value;
	public final MultiKey owner;

	public Key(BigInteger value) {
		this.value = new Identifier(value);
		this.owner = null;
	}

	public Key(BigInteger value, MultiKey owner) {
		this.value = new Identifier(value);
		this.owner = owner;
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
