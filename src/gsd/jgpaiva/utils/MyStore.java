package gsd.jgpaiva.utils;

import gsd.jgpaiva.interfaces.RunnableWithArgs;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;


public class MyStore<T> implements Collection<T> {
	private static class Container<T> {
		public Container() {
		}

		Container<T> next;
		T el;
	}

	int size = 0;
	Container<T> head = new Container<T>();
	Container<T> tail = this.head;

	private final int compare(Object k1, Object k2) {
		return ((Comparable<? super T>) k1).compareTo((T) k2);
	}

	@Override
	public void clear() {
		this.head.next = null;
		this.tail = this.head;
		this.size = 0;
	}

	@Override
	public int size() {
		return this.size;
	}

	@Override
	public boolean isEmpty() {
		return this.size == 0;
	}

	@Override
	public boolean add(T e) {
		Container<T> toAdd = new Container<T>();
		toAdd.el = e;
		assert (this.tail.el == null || this.compare(this.tail.el, e) < 0);
		this.tail.next = toAdd;
		this.tail = toAdd;
		this.size++;
		return true;
	}

	boolean add(Container<T> toAdd) {
		assert (this.tail.el == null || this.compare(this.tail.el, toAdd.el) < 0);
		toAdd.next = null;
		this.tail.next = toAdd;
		this.tail = toAdd;
		this.size++;
		return true;
	}

	public int moveToMe(MyStore<T> other) {
		if (other.size == 0) return 0;
		if (this.size == 0) {
			this.moveAll(other.size, other.head, this.tail, other);
			assert (other.size != 0 || other.head.next == null);
			assert (this.size != 0 || this.head.next == null);
			assert (this.tail.next == null);
			assert (other.tail.next == null);
			return this.size;
		}
		int initialMySize = this.size;
		int initialOtherSize = other.size;
		// declare values
		Container<T> thisOld = this.head;
		Container<T> otherOld = other.head;
		Container<T> thisCurrent = this.head.next;
		Container<T> otherCurrent = other.head.next;
		int otherRemaining = other.size;
		int moved = 0;
		boolean done = false;
		while (true) {
			int result = this.compare(thisCurrent.el, otherCurrent.el);
			if (result == 0) {
				// both contain the element. advance both
				if (otherCurrent.next != null) {
					otherOld = otherCurrent;
					otherCurrent = otherCurrent.next;
					otherRemaining--;
				} else {
					done = true;
					break;
				}
				if (thisCurrent.next != null) {
					thisOld = thisCurrent;
					thisCurrent = thisCurrent.next;
				} else {
					break;
				}
			} else if (result > 0) {
				// mine is larger than the other, I've missed a bunch
				// System.out.println("XXX: moving " + otherCurrent.el +
				// " since it's smaller than "+ thisCurrent.el);
				otherOld.next = otherCurrent.next;
				other.size--;
				thisOld.next = otherCurrent;
				otherCurrent.next = thisCurrent;
				this.size++;
				moved++;
				thisOld = otherCurrent;
				if (other.tail == otherCurrent) {
					other.tail = otherOld;
				}

				if (otherOld.next != null) {
					otherCurrent = otherOld.next;
					otherRemaining--;
				} else {
					done = true;
					break;
				}
			} else {
				// mine is smaller than other, should advance
				if (thisCurrent.next != null) {
					thisOld = thisCurrent;
					thisCurrent = thisCurrent.next;
				} else {
					break;
				}
			}
		}
		if (!done) {
			this.moveAll(otherRemaining, otherOld, thisCurrent, other);
			moved += otherRemaining;
		}
		assert (this.size == initialMySize + moved);
		assert (other.size == initialOtherSize - moved);
		assert (other.size != 0 || other.head.next == null);
		assert (this.size != 0 || this.head.next == null);
		assert (this.tail.next == null);
		assert (other.tail.next == null);
		// this.runTest(this,other);
		return moved;
	}

	private void runTest(MyStore<T> myStore, MyStore<T> other) {
		HashSet<Container<T>> hset = new HashSet<MyStore.Container<T>>();
		Container<T> current = myStore.head;
		while (current != null) {
			boolean result = hset.add(current);
			assert (result);
			current = current.next;
		}
		current = other.head;
		while (current != null) {
			boolean result = hset.add(current);
			assert (result);
			current = current.next;
		}
	}

	private void moveAll(int toMove, Container<T> fromPrevious, Container<T> to, MyStore<T> other) {
		other.size -= toMove;
		this.size += toMove;
		assert (to.next == null);
		to.next = fromPrevious.next;
		fromPrevious.next = null;
		this.tail = other.tail;
		other.tail = fromPrevious;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public Iterator<T> iterator() {
		return new MyStoreIterator<T>() {
			Container<T> prevItem = null;
			Container<T> item = MyStore.this.head;
			int seenItems = 0;

			@Override
			public boolean hasNext() {
				assert (this.seenItems < MyStore.this.size || this.item.next == null);
				return this.seenItems < MyStore.this.size;
			}

			@Override
			public T next() {
				if (!this.hasNext()) throw new NoSuchElementException();

				this.prevItem = this.item;
				this.item = this.item.next;
				this.seenItems++;
				return this.item.el;
			}

			@Override
			public void remove() {
				if (this.seenItems == 0 || this.prevItem == this.item)
					throw new NoSuchElementException("next() was not called");
				assert (this.prevItem != null);

				this.prevItem.next = this.item.next;
				// System.out.println("rm " + this.item.el + " " +
				// this.prevItem.el + " "+ MyStore.this.size + " " +
				// this.seenItems);
				this.item = this.prevItem;
				if (this.prevItem.next == null) {
					MyStore.this.tail = this.prevItem;
				}
				MyStore.this.size--;
				this.seenItems--;
			}

			@Override
			public void moveTo(MyStore<T> other) {
				if (this.seenItems == 0 || this.prevItem == this.item)
					throw new NoSuchElementException("next() was not called");
				assert (this.prevItem != null);
				assert (other != MyStore.this);

				this.prevItem.next = this.item.next;
				other.add(this.item);
				this.item = this.prevItem;
				if (this.prevItem.next == null) {
					MyStore.this.tail = this.prevItem;
				}
				MyStore.this.size--;
				this.seenItems--;
			}
		};
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		for (T it : c) {
			this.add(it);
		}
		return c.size() > 0;
	}

	@Override
	public boolean removeAll(Collection<?> other) {
		if (other.size() == 0 || this.size == 0) return false;
		int initialMySize = this.size;
		int initialOtherSize = other.size();
		// declare values
		Container<T> thisOld = this.head;
		Container<T> thisCurrent = this.head.next;
		Iterator<?> otherIt = other.iterator();
		Object otherEl = otherIt.next();
		int removed = 0;
		while (true) {
			int result = this.compare(thisCurrent.el, otherEl);
			if (result == 0) {
				// both contain the element. remove
				thisCurrent = thisCurrent.next;
				thisOld.next = thisCurrent;
				this.size--;
				removed++;

				if (thisCurrent == null) {
					this.tail = thisOld;
					break;
				}
				if (otherIt.hasNext()) {
					otherEl = otherIt.next();
				} else {
					break;
				}
			} else if (result > 0) {
				// other is smaller than mine, should advance
				if (otherIt.hasNext()) {
					otherEl = otherIt.next();
				} else {
					break;
				}
			} else {
				// mine is smaller than other, should advance
				if (thisCurrent.next != null) {
					thisOld = thisCurrent;
					thisCurrent = thisCurrent.next;
				} else {
					break;
				}
			}
		}
		assert (this.size + removed == initialMySize);
		assert (other.size() == initialOtherSize);
		assert (this.size != 0 || this.head.next == null);
		assert (this.tail.next == null);

		return removed > 0;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not yet implemented");
	}

	public static void main(String[] args) {
		MyStore<Integer> a = new MyStore<Integer>();

		a.add(1);
		a.add(2);
		a.add(3);
		a.add(4);
		a.add(5);

		System.out.println("a" + a);

		MyStore<Integer> b = new MyStore<Integer>();

		b.add(2);
		b.add(6);

		System.out.println("b" + b);

		int res = b.moveToMe(a);

		System.out.println("a" + a);
		System.out.println("b" + b);

		System.out.println(res);

		a.add(3);
		a.add(6);
		a.add(7);

		b.add(9);

		System.out.println("a" + a);
		System.out.println("b" + b);
		MyStore<Integer> result = b.removeAllMatching(a);

		System.out.println("a" + a);
		System.out.println("b" + b);
		System.out.println("res" + result);

		MyStore<Integer> c = new MyStore<Integer>();

		c.moveToMe(b);

		System.out.println("c" + c);

		System.out.println("b" + b);

		c.moveToMe(b);

		System.out.println("c" + c);

		System.out.println("b" + b);

		MyStore<Integer> d = new MyStore<Integer>();

		d.add(1);
		d.add(2);
		d.add(3);
		d.add(4);
		d.add(5);

		Iterator<Integer> it = d.iterator();
		while (it.hasNext()) {
			if (it.next() == 3) {
				it.remove();
			}
		}
		System.out.println("d" + d);
		it = d.iterator();
		while (it.hasNext()) {
			Integer val = it.next();
			if (val == 1) {
				it.remove();
			}
			if (val == 2) {
				it.remove();
			}
			if (val == 5) {
				it.remove();
			}
		}
		System.out.println("d" + d);
		it = d.iterator();
		while (it.hasNext()) {
			if (it.next() == 4) {
				it.remove();
			}
		}
		System.out.println("d" + d);

		RunnableWithArgs funct = new RunnableWithArgs() {
			@Override
			public void run(Object args) {
			}
		};
		res = d.addAll(c, funct);

		System.out.println("d" + d);
		System.out.println(res);

		res = d.addAll(a, funct);

		System.out.println("d" + d);
		System.out.println(res);

		res = d.addAll(a, funct);

		System.out.println("d" + d);
		System.out.println(res);

	}

	@Override
	public String toString() {
		String toRet = "[ ";
		for (T it : this) {
			toRet += it + " ";
		}
		toRet += "]";
		return toRet;
	}

	public MyStore<T> removeAllMatching(MyStore<T> other) {
		MyStore<T> toRet = new MyStore<T>();

		if (other.size == 0 || this.size == 0) return toRet;
		int initialMySize = this.size;
		int initialOtherSize = other.size;
		// declare values
		Container<T> thisOld = this.head;
		Container<T> otherOld = other.head;
		Container<T> thisCurrent = this.head.next;
		Container<T> otherCurrent = other.head.next;
		while (true) {
			int result = this.compare(thisCurrent.el, otherCurrent.el);
			if (result == 0) {
				// both contain the element. remove
				thisCurrent = thisCurrent.next;
				// System.out.println("rm " + thisOld.next.el);
				toRet.add(thisOld.next);
				thisOld.next = thisCurrent;
				this.size--;

				if (thisCurrent == null) {
					this.tail = thisOld;
					break;
				}

				if (otherCurrent.next != null) {
					otherOld = otherCurrent;
					otherCurrent = otherCurrent.next;
				} else {
					break;
				}
			} else if (result > 0) {
				// other is smaller than mine, should advance
				if (otherCurrent.next != null) {
					otherOld = otherCurrent;
					otherCurrent = otherOld.next;
				} else {
					break;
				}
			} else {
				// mine is smaller than other, should advance
				if (thisCurrent.next != null) {
					thisOld = thisCurrent;
					thisCurrent = thisCurrent.next;
				} else {
					break;
				}
			}
		}
		assert (this.size + toRet.size == initialMySize);
		assert (other.size == initialOtherSize);
		assert (other.size != 0 || other.head.next == null);
		assert (this.size != 0 || this.head.next == null);
		assert (this.tail.next == null);
		assert (other.tail.next == null);

		return toRet;
	}

	public int moveToMe(MyStore<T> other, RunnableWithArgs funct) {
		if (other.size == 0) return 0;

		if (this.size == 0) {
			this.moveAll(other.size, other.head, this.tail, other, funct);
			assert (other.size != 0 || other.head.next == null);
			assert (this.size != 0 || this.head.next == null);
			assert (this.tail.next == null);
			assert (other.tail.next == null);
			return this.size;
		}
		int initialMySize = this.size;
		int initialOtherSize = other.size;
		// declare values
		Container<T> thisOld = this.head;
		Container<T> otherOld = other.head;
		Container<T> thisCurrent = this.head.next;
		Container<T> otherCurrent = other.head.next;
		int otherRemaining = other.size;
		int moved = 0;
		boolean done = false;
		while (true) {
			int result = this.compare(thisCurrent.el, otherCurrent.el);
			if (result == 0) {
				// both contain the element. advance both
				if (otherCurrent.next != null) {
					otherOld = otherCurrent;
					otherCurrent = otherCurrent.next;
					otherRemaining--;
				} else {
					done = true;
					break;
				}
				if (thisCurrent.next != null) {
					thisOld = thisCurrent;
					thisCurrent = thisCurrent.next;
				} else {
					break;
				}
			} else if (result > 0) {
				// mine is larger than the other, I've missed a bunch
				// System.out.println("XXX: moving " + otherCurrent.el +
				// " since it's smaller than "+ thisCurrent.el);
				otherOld.next = otherCurrent.next;
				other.size--;
				thisOld.next = otherCurrent;
				otherCurrent.next = thisCurrent;
				this.size++;
				moved++;
				thisOld = otherCurrent;
				funct.run(otherCurrent.el);

				if (other.tail == otherCurrent) {
					other.tail = otherOld;
				}

				if (otherOld.next != null) {
					otherCurrent = otherOld.next;
					otherRemaining--;
				} else {
					done = true;
					break;
				}
			} else {
				// mine is smaller than other, should advance
				if (thisCurrent.next != null) {
					thisOld = thisCurrent;
					thisCurrent = thisCurrent.next;
				} else {
					break;
				}
			}
		}
		if (!done) {
			this.moveAll(otherRemaining, otherOld, thisCurrent, other, funct);
			moved += otherRemaining;
		}
		assert (this.size == initialMySize + moved);
		assert (other.size == initialOtherSize - moved);
		assert (other.size != 0 || other.head.next == null);
		assert (this.size != 0 || this.head.next == null);
		assert (this.tail.next == null);
		assert (other.tail.next == null);
		// this.runTest(this,other);
		return moved;
	}

	private void moveAll(int toMove, Container<T> fromPrevious, Container<T> to, MyStore<T> other,
			RunnableWithArgs funct) {
		int touched = 0;
		for (Container<T> current = fromPrevious.next; current != null; current = current.next) {
			funct.run(current.el);
			touched++;
		}
		assert (touched == toMove);

		other.size -= toMove;
		this.size += toMove;
		assert (to.next == null);
		to.next = fromPrevious.next;
		fromPrevious.next = null;
		this.tail = other.tail;
		other.tail = fromPrevious;
	}

	public int addAll(MyStore<T> other, RunnableWithArgs funct) {
		if (other.size == 0) return 0;

		int initialMySize = this.size;
		int initialOtherSize = other.size;
		// declare values
		Container<T> thisOld = this.head;
		Container<T> thisCurrent = this.head.next;
		Container<T> otherCurrent = other.head.next;
		int copied = 0;
		assert (otherCurrent != null);
		while (otherCurrent != null) {
			if (thisCurrent == null) {
				for (; otherCurrent != null; otherCurrent = otherCurrent.next) {
					thisOld.next = new Container<T>();
					thisOld = thisOld.next;
					thisOld.el = otherCurrent.el;
					this.size++;
					copied++;
					funct.run(otherCurrent.el);
				}
				this.tail = thisOld;
				break;
			}

			assert (otherCurrent != null);
			int result = this.compare(thisCurrent.el, otherCurrent.el);
			if (result == 0) {
				// both contain the element. advance both
				otherCurrent = otherCurrent.next;

				thisOld = thisCurrent;
				thisCurrent = thisCurrent.next;
			} else if (result > 0) {
				// mine is larger than the other, I've missed a bunch
				// System.out.println("XXX: moving " + otherCurrent.el +
				// " since it's smaller than "+ thisCurrent.el);
				thisOld.next = new Container<T>();
				thisOld = thisOld.next;
				thisOld.el = otherCurrent.el;
				thisOld.next = thisCurrent;
				this.size++;
				copied++;
				funct.run(otherCurrent.el);

				otherCurrent = otherCurrent.next;
			} else {
				// mine is smaller than other, should advance
				thisOld = thisCurrent;
				thisCurrent = thisCurrent.next;
			}
		}

		assert (this.size == initialMySize + copied);
		assert (other.size == initialOtherSize);
		assert (this.size == initialMySize + other.size);
		assert (other.size != 0 || other.head.next == null);
		assert (this.size != 0 || this.head.next == null);
		assert (this.tail.next == null);
		assert (other.tail.next == null);
		return copied;
	}
}