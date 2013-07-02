package gsd.jgpaiva.utils;

import java.util.Iterator;

public interface MyStoreIterator<T> extends Iterator<T> {
	public void moveTo(MyStore<T> other);
}
