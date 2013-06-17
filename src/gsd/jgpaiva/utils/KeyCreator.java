package gsd.jgpaiva.utils;

import gsd.jgpaiva.structures.dht.Finger;
import gsd.jgpaiva.structures.replication.ComplexKey;
import gsd.jgpaiva.structures.replication.CountableKey;
import gsd.jgpaiva.structures.replication.Key;
import gsd.jgpaiva.structures.replication.MultiKey;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

import peersim.config.Configuration;
import peersim.core.CommonState;

public class KeyCreator {
	private final String prefix = "keyCreator"; // martelada!
	private static final String PAR_N_KEYS = "nkeys";
	private static final String PAR_MODE = "initmode";
	// Values allowed to parameter initialization mode
	private static final String MOD_RAND = "random";
	private static final String MOD_LINEAR = "linear";
	private static final String MOD_NON_LINEAR = "nonlinear";
	private static final String MOD_READ = "read";
	private static final String PAR_READ_FILE = "readfile";
	private static final String PAR_MULTI_KEY = "multikey";

	private enum InitMode {
		MOD_RAND, MOD_LINEAR, MOD_NON_LINEAR, MOD_READ
	}

	public enum KeyMode {
		COMPLEX_KEY, LOAD_KEY, REGULAR_KEY, MULTI_KEY
	}

	// private static boolean multiKey;

	private final int nKeys;
	private final int idLength;
	private InitMode initMode;
	private String fileToRead;

	private TreeSet<Key> allKeys;
	private int totalLoad;
	private Key[] allKeysArray;
	private final ArrayList<MultiKey> allMultiKeys;
	// private final boolean useComplexKeys;
	private KeyMode keyMode;
	private int totalKeys;

	private static KeyCreator instance;

	public static KeyCreator initialize(KeyMode mode) {
		if (KeyCreator.instance == null) {
			KeyCreator.instance = new KeyCreator(mode);
		} else {
			throw new RuntimeException("Should never happen: double initialization");
		}
		return KeyCreator.instance;
	}

	private KeyCreator(KeyMode keyMode) {
		this.nKeys = Configuration.getInt(this.prefix + "." + KeyCreator.PAR_N_KEYS);
		this.idLength = GlobalConfig.getIdLength();
		if (Configuration.contains(this.prefix + "." + KeyCreator.PAR_MULTI_KEY) ? Configuration
				.getBoolean(this.prefix + "." + KeyCreator.PAR_MULTI_KEY)
				: false)
			keyMode = KeyMode.MULTI_KEY;

		this.keyMode = keyMode;
		String mode = Configuration.getString(this.prefix + "." + KeyCreator.PAR_MODE);
		if (mode.compareTo(KeyCreator.MOD_RAND) == 0) {
			this.initMode = InitMode.MOD_RAND;
		} else if (mode.compareTo(KeyCreator.MOD_LINEAR) == 0) {
			this.initMode = InitMode.MOD_LINEAR;
		} else if (mode.compareTo(KeyCreator.MOD_NON_LINEAR) == 0) {
			this.initMode = InitMode.MOD_NON_LINEAR;
		} else if (mode.compareTo(KeyCreator.MOD_READ) == 0) {
			this.initMode = InitMode.MOD_READ;
			this.fileToRead = Configuration.getString(this.prefix + "." + KeyCreator.PAR_READ_FILE);
			this.keyMode = KeyMode.LOAD_KEY;
		} else {
			this.initMode = InitMode.MOD_RAND; // Default
		}
		this.allKeys = new TreeSet<Key>();
		this.allMultiKeys = new ArrayList<MultiKey>();
		this.allKeysArray = null;

		this.createKeys();
		this.allKeysArray = this.allKeys.toArray(new Key[0]);
		this.totalKeys = allKeysArray.length;
		// this.allKeys = null;
	}

	private void createKeys() {
		if (this.allKeys.size() == 0) {
			switch (this.initMode) {
			case MOD_LINEAR:
			case MOD_NON_LINEAR:
			case MOD_RAND:
				for (int i = 0; i < this.nKeys; i++) {
					BigInteger current = new BigInteger(this.idLength, CommonState.r);
					this.createNewKey(current);
				}
				break;
			case MOD_READ:
				Scanner sc;
				try {
					sc = new Scanner(new File(this.fileToRead));
				} catch (FileNotFoundException e) {
					throw new RuntimeException(e);
				}
				sc.skip("#.*\n");
				sc.useDelimiter(",");
				int counter = 0;
				while (sc.hasNext()) {
					String tk = sc.next();
					int load = Integer.parseInt(tk);
					BigInteger current = BigInteger.valueOf(counter++);
					this.createNewKey(current, load);
					totalLoad += load;
				}
				System.err.println("Loaded " + this.allKeys.size() +
						" keys from " + fileToRead + " for a total load of " + totalLoad);
			}

		}
	}

	private void createNewKey(BigInteger value, int load) {
		if (this.keyMode == KeyMode.LOAD_KEY) {
			if (!this.allKeys.add(new Key(value, load)))
				throw new RuntimeException("Key was in treeset!");
		}
	}

	private void createNewKey(BigInteger value) {
		if (this.keyMode == KeyMode.MULTI_KEY) {
			MultiKey temp = new MultiKey(value);
			if (!this.allMultiKeys.add(temp))
				throw new RuntimeException("MultiKey was in treeset!");
			for (Key key : temp.values)
				if (!this.allKeys.add(key))
					throw new RuntimeException("Key was in treeset!");
		} else if (this.keyMode == KeyMode.COMPLEX_KEY) {
			if (!this.allKeys.add(new ComplexKey(value)))
				throw new RuntimeException("Key was in treeset!");
		} else {
			if (!this.allKeys.add(new CountableKey(value)))
				throw new RuntimeException("Key was in treeset!");
		}
	}

	public int getRangeSize(Finger lowerFinger, Finger higherFinger) {
		if (lowerFinger == higherFinger)
			return this.allKeys.size();

		BigInteger lowerID = lowerFinger.id;
		BigInteger higherID = higherFinger.id;
		if (lowerID.compareTo(higherID) <= 0) {
			Set<Key> set = this.allKeys.subSet(new Key(lowerID), false, new Key(higherID), true);
			return set.size();
		} else {
			Set<Key> set1 = this.allKeys.headSet(new Key(higherID), true);
			Set<Key> set2 = this.allKeys.tailSet(new Key(lowerID), false);
			return set1.size() + set2.size();
		}
	}

	public int getNKeys() {
		return this.nKeys;
	}

	public Key[] getKeyArray() {
		return this.allKeysArray;
	}

	public Collection<? extends Key> getRangeBottom(Finger lowerFinger, Finger higherFinger) {
		assert (lowerFinger != higherFinger);

		BigInteger lowerID = lowerFinger.id;
		BigInteger higherID = higherFinger.id;
		if (lowerID.compareTo(higherID) <= 0)
			return this.allKeys.subSet(new Key(lowerID), false, new Key(higherID), true);
		else
			return this.allKeys.headSet(new Key(higherID), true);
	}

	public Collection<? extends Key> getRangeTop(Finger lowerFinger, Finger higherFinger) {
		assert (lowerFinger != higherFinger);

		BigInteger lowerID = lowerFinger.id;
		BigInteger higherID = higherFinger.id;
		if (lowerID.compareTo(higherID) <= 0)
			return Collections.emptySet();
		else
			return this.allKeys.tailSet(new Key(lowerID), false);
	}

	public TreeSet<Key> getAllKeys() {
		return this.allKeys;
	}

	public int getTotalLoad() {
		return this.totalLoad;
	}

	public int getTotalKeys() {
		return this.totalKeys;
	}
}
