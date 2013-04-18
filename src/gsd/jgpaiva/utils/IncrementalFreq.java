/*
 * Copyright (c) 2007 Joao Leitao - GSD/INESC-ID
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 */

/**
 * Simplified version of the peersim version of this class.
 * It now allows for the use of long and it uses a HashMap for memory limitations constraints
 */

package gsd.jgpaiva.utils;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

/**
 * A class that can collect frequency information on Long input.
 */
public class IncrementalFreq implements Cloneable {

	// ===================== fields ========================================
	// =====================================================================

	/** The number of items inserted. */
	private int n;

	/** freq holds the frequency of each key. */
	private HashMap<Long, Integer> freq = null;

	// ====================== initialization ==============================
	// ====================================================================

	// --------------------------------------------------------------------

	public IncrementalFreq() {
		this.n = 0;
		this.freq = new HashMap<Long, Integer>();
	}

	// --------------------------------------------------------------------

	/**
	 * Reset the state of the object. After calling this, all public methods
	 * behave the same as they did after constructing the object.
	 */
	public void reset() {
		this.n = 0;
		this.freq = new HashMap<Long, Integer>();
	}

	// ======================== methods ===================================
	// ====================================================================

	/**
	 * Adds item <code>i</code> to the input set. It calls <code>add(i,1)</code>
	 * .
	 * 
	 * @see #add(int,int)
	 */
	public final void add(long i) {
		this.add(i, 1);
	}

	// --------------------------------------------------------------------

	/**
	 * Adds item <code>i</code> to the input set <code>k</code> times. That is,
	 * it increments counter <code>i</code> by <code>k</code>. If, however,
	 * <code>i</code> is negative, or larger than the maximum defined at
	 * construction time (if a maximum was set at all) the operation is ignored.
	 */
	public void add(long i, int k) {

		if (k <= 0) return;

		// Increase number of items by k.
		this.n += k;

		Long key = new Long(i);

		// check if element i already been registered before
		if (this.freq.containsKey(key)) {
			this.freq.put(key, new Integer(this.freq.get(key).intValue() + k));
		} else {
			this.freq.put(key, new Integer(k));
		}
	}

	// --------------------------------------------------------------------

	/**
	 * Returns number of processed data items. This is the number of items over
	 * which the class holds statistics.
	 */
	public int getN() {
		return this.n;
	}

	// --------------------------------------------------------------------

	/** Returns the number of occurrences of the given integer. */
	public int getFreq(long i) {
		Long key = new Long(i);

		if (this.freq.containsKey(key))
			return this.freq.get(key).intValue();
		else
			return 0;
	}

	// --------------------------------------------------------------------

	/**
	 * Prints current frequency information. Prints a separate line for all
	 * values from 0 to the capacity of the internal representation using the
	 * format
	 * 
	 * <pre>
	 * value occurences
	 * </pre>
	 * 
	 * That is, numbers with zero occurrences will also be printed.
	 */
	public void printAll(PrintStream out) {

		Iterator<Long> ite = new TreeSet<Long>(this.freq.keySet()).iterator();

		Long key = null;

		while (ite.hasNext())
		{
			key = ite.next();
			out.println(key.longValue() + " " + this.freq.get(key).intValue());
		}
	}

	// ---------------------------------------------------------------------

	/**
	 * Prints current frequency information. Prints a separate line for all
	 * values that have a number of occurrences different from zero using the
	 * format
	 * 
	 * <pre>
	 * value occurences
	 * </pre>
	 */
	public void print(PrintStream out) {
		this.printAll(out);
	}

	// ---------------------------------------------------------------------

	@Override
	public String toString() {

		String result = "";

		Iterator<Long> ite = new TreeSet<Long>(this.freq.keySet()).iterator();

		Long key = null;

		while (ite.hasNext())
		{
			key = ite.next();
			result = result + " (" + key.longValue() + "," + this.freq.get(key).intValue() + ")";
		}
		return result;
	}

	// ---------------------------------------------------------------------

	@Override
	@SuppressWarnings("unchecked")
	public Object clone() throws CloneNotSupportedException {

		IncrementalFreq result = (IncrementalFreq) super.clone();
		if (this.freq != null) {
			result.freq = (HashMap<Long, Integer>) this.freq.clone();
		}
		return result;
	}

	// ---------------------------------------------------------------------

	/**
	 * Tests equality between two IncrementalFreq instances. Two objects are
	 * equal if both hold the same set of numbers that have occurred non-zero
	 * times and the number of occurrences is also equal for these numbers.
	 */
	@Override
	public boolean equals(Object obj) {
		boolean ans = true;

		IncrementalFreq other = (IncrementalFreq) obj;

		Iterator<Long> ite = new TreeSet<Long>(this.freq.keySet()).iterator();

		Long key = null;

		while (ite.hasNext())
		{
			key = ite.next();
			if (!other.freq.containsKey(key)) {
				ans = false;
				break;
			}

			if (this.freq.get(key).intValue() != other.freq.get(key).intValue()) {
				ans = false;
				break;
			}
		}

		return ans;

	}

	public void normalize() {
		HashMap<Long, Integer> copy = (HashMap<Long, Integer>) this.freq.clone();
		for (Entry<Long, Integer> it : copy.entrySet()) {
			this.freq.put(it.getKey(),
					(int) (it.getKey() != 0 ? it.getValue() / it.getKey() : it.getValue()));
		}
	}

}
