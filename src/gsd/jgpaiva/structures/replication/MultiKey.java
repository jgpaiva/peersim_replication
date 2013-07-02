package gsd.jgpaiva.structures.replication;

import gsd.jgpaiva.utils.GlobalConfig;

import java.math.BigInteger;

import peersim.config.Configuration;
import peersim.core.CommonState;

public class MultiKey {

	public static MultiKey[] allMultiKeys;

	public Key[] values;

	public static final String name = "multiKey";
	public static final int replication = Configuration.getInt(MultiKey.name
			+ '.'
			+ "replication");
	public static final int idLength = GlobalConfig.getIdLength();

	public MultiKey(BigInteger value) {
		this.values = new Key[MultiKey.replication];
		this.values[0] = new CountableKey(value, this);
		for (int it = 1; it < MultiKey.replication; it++) {
			this.values[it] = new CountableKey(new BigInteger(MultiKey.idLength,
					CommonState.r), this);
		}
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof MultiKey ? this.values == ((MultiKey) obj).values
				: false;
	}

	@Override
	public int hashCode() {
		if (this.values == null)
			return 0;
		return this.values.hashCode();
	}
}
