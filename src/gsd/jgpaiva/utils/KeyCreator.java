package gsd.jgpaiva.utils;

import gsd.jgpaiva.structures.dht.Finger;
import gsd.jgpaiva.structures.replication.ComplexKey;
import gsd.jgpaiva.structures.replication.CountableKey;
import gsd.jgpaiva.structures.replication.Key;
import gsd.jgpaiva.structures.replication.MultiKey;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

	private static boolean multiKey;

	private final int nKeys;
	private final int idLength;
	private InitMode initMode;
	private String fileToRead;

	private TreeSet<Key> allKeys;
	private final Key[] allKeysArray;
	private final ArrayList<MultiKey> allMultiKeys;
	private final boolean useComplexKeys;

	private static KeyCreator instance;

	public static KeyCreator getInstance() {
		if (KeyCreator.instance == null) {
			KeyCreator.instance = new KeyCreator(false);
		}
		return KeyCreator.instance;
	}

	public static KeyCreator getInstance(boolean useComplexKeys) {
		if (KeyCreator.instance == null) {
			KeyCreator.instance = new KeyCreator(useComplexKeys);
		}
		return KeyCreator.instance;
	}

	private KeyCreator(boolean useComplexKeys) {
		this.nKeys = Configuration.getInt(this.prefix + "." + KeyCreator.PAR_N_KEYS);
		this.idLength = GlobalConfig.getIdLength();
		KeyCreator.multiKey = Configuration.contains(this.prefix + "." + KeyCreator.PAR_MULTI_KEY) ? Configuration
				.getBoolean(this.prefix + "." + KeyCreator.PAR_MULTI_KEY) : false;
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
		} else {
			this.initMode = InitMode.MOD_RAND; // Default
		}
		this.allKeys = new TreeSet<Key>();
		this.allMultiKeys = new ArrayList<MultiKey>();
		this.allKeysArray = null;
		this.useComplexKeys = useComplexKeys;

		this.createKeys();
		// this.allKeysArray = this.allKeys.toArray(new Key[0]);
		// this.allKeys = null;
	}

	private void createKeys() {
		if (this.allKeys.size() == 0) {
			switch (this.initMode) {
			case MOD_LINEAR:
			case MOD_NON_LINEAR:
			case MOD_READ:
			case MOD_RAND:
				for (int i = 0; i < this.nKeys; i++) {
					BigInteger current = new BigInteger(this.idLength, CommonState.r);
					this.createNewKey(current);
				}
			}
		}
	}

	private void createNewKey(BigInteger value) {
		if (!KeyCreator.multiKey) {
			if (this.useComplexKeys) {
				if (!this.allKeys.add(new ComplexKey(value)))
					throw new RuntimeException("Key was in treeset!");
			} else {
				if (!this.allKeys.add(new CountableKey(value)))
					throw new RuntimeException("Key was in treeset!");
			}
		} else {
			MultiKey temp = new MultiKey(value);
			if (!this.allMultiKeys.add(temp))
				throw new RuntimeException("MultiKey was in treeset!");
			for (Key key : temp.values)
				if (!this.allKeys.add(key)) throw new RuntimeException("Key was in treeset!");
		}
	}

	public int getRangeSize(Finger lowerFinger, Finger higherFinger) {
		if (lowerFinger == higherFinger) return this.allKeys.size();

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
}
