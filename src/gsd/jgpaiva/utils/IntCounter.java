package gsd.jgpaiva.utils;

public class IntCounter {
	public int value;

	public IntCounter() {
		this.value = 1;
	}

	public IntCounter(int value) {
		this.value = value;
	}

	public void incr() {
		this.value++;
	}
}