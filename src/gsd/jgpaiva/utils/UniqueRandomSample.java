package gsd.jgpaiva.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

/**
 * Efficient computation of random and unique elements from a set. Class not
 * synchronized
 */
public class UniqueRandomSample implements Iterable<Integer> {
	private int[] buffer;
	private int last;
	private Random r;

	/**
	 * build random sample with size size and internally using random number
	 * generator r
	 * 
	 * @param size
	 * @param r
	 */
	public UniqueRandomSample(int size, Random r) {
		this.buffer = new int[size];
		this.r = r;
		this.init();
	}

	/**
	 * reset the samples
	 */
	public void init() {
		this.last = this.buffer.length - 1;
		for (int i = 0; i < this.buffer.length; i++) {
			this.buffer[i] = i;
		}
	}

	/**
	 * reset the samples
	 */
	public void reset() {
		this.last = this.buffer.length - 1;
	}

	public int next() {
		if (!this.hasNext())
			throw new RuntimeException("No more samples");

		int index = this.r.nextInt(this.last + 1);
		int toReturn = this.buffer[index];
		int temp = this.buffer[this.last];
		this.buffer[this.last] = toReturn;
		this.buffer[index] = temp;
		this.last--;
		return toReturn;
	}

	public ArrayList<Integer> getNotUsed() {
		ArrayList<Integer> toReturn = new ArrayList<Integer>(this.last);
		for (int i = 0; i <= this.last; i++) {
			toReturn.add(this.buffer[i]);
		}
		return toReturn;
	}

	public boolean hasNext() {
		return this.last != -1;
	}

	@Override
	public Iterator<Integer> iterator() {
		return new Iterator<Integer>() {
			@Override
			public boolean hasNext() {
				return UniqueRandomSample.this.hasNext();
			}

			@Override
			public Integer next() {
				return UniqueRandomSample.this.next();
			}

			@Override
			public void remove() {
				// TODO Auto-generated method stub
				throw new RuntimeException("Not supported");
			}

		};
	}

	public static void main(String[] args) {
		UniqueRandomSample samples = new UniqueRandomSample(10, new Random());

		System.out.println("using cycle");
		while (samples.hasNext()) {
			System.out.println(samples.next());
		}

		System.out.println("using iterator");
		samples.reset();
		for (int it : samples) {
			System.out.println(it);
		}
	}
}
