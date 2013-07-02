package gsd.jgpaiva.utils;

public class Pair<T1, T2> {
	public T1 fst;
	public T2 snd;

	public Pair(T1 fst, T2 snd) {
		this.fst = fst;
		this.snd = snd;
	}

	@Override
	public String toString() {
		return "(" + this.fst + "," + this.snd + ")";
	}

	public boolean equals(Pair<T1, T2> arg) {
		return this.fst.equals(arg.fst) && this.snd.equals(arg.snd);
	}
}
