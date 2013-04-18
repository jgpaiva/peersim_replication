package gsd.jgpaiva.utils;

public class LongCounter {
	public long value;

	public LongCounter() {
		this.value = 1;
	}

	public LongCounter(long value) {
		this.value = value;
	}

	public void incr() {
		this.value++;
	}
}