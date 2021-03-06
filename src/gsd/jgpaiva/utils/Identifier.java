package gsd.jgpaiva.utils;

import java.math.BigInteger;
import java.util.Random;

public class Identifier extends BigInteger {

	public Identifier(String val, int radix) {
		super(val, radix);
		// TODO Auto-generated constructor stub
	}

	public Identifier(String val) {
		super(val);
		// TODO Auto-generated constructor stub
	}

	public Identifier(int numBits, Random rnd) {
		super(numBits, rnd);
		// TODO Auto-generated constructor stub
	}

	public Identifier(int bitLength, int certainty, Random rnd) {
		super(bitLength, certainty, rnd);
		// TODO Auto-generated constructor stub
	}

	public Identifier(int signum, byte[] magnitude) {
		super(signum, magnitude);
		// TODO Auto-generated constructor stub
	}

	public Identifier(byte[] val) {
		super(val);
		// TODO Auto-generated constructor stub
	}

	public Identifier(BigInteger value) {
		super(value.toByteArray());
	}

	/**
	 *
	 */
	private static final long serialVersionUID = 4355742652894030110L;

	@Override
	public String toString() {
		return super.toString(Character.MAX_RADIX);
	}
}
