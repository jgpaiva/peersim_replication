package gsd.jgpaiva.utils;




import gsd.jgpaiva.structures.MyNode;

import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;

import peersim.core.CommonState;
import peersim.core.Node;

public class Utils {
	
/*	
 * 
import gsd.protocols.dht.Finger;
import gsd.protocols.vsp.dht.Mapping; 
  
  public static Finger getNextFinger(TreeSet<Finger> set, Finger myFinger) {
 
		if (set.contains(myFinger))
			throw new RuntimeException("This should NEVER happen!");

		if (set.size() > 0) {
			Finger toReturn = null;
			set.add(myFinger);
			for (Iterator<Finger> it = set.iterator(); it.hasNext();) {
				if (it.next() == myFinger) {
					if (it.hasNext()) {
						toReturn = it.next();
					} else {
						toReturn = set.first();
					}
				}
			}
			set.remove(myFinger);
			return toReturn;
		} // else
		return null;
	}

	public static Finger getPreviousFinger(TreeSet<Finger> set, Finger myFinger) {
		if (set.contains(myFinger))
			throw new RuntimeException("This should NEVER happen!");

		if (set.size() > 0) {
			Finger toReturn = null;
			set.add(myFinger);
			for (Finger current : set) {
				if (current == myFinger) {
					break;
				}
				toReturn = current;
			}
			set.remove(myFinger);
			if (toReturn == null) {
				toReturn = set.last();
			}
			return toReturn;
		} // else
		return null;
	}

	*
	 * get a bit mask with 1 at higher num positions.
	 * 
	 * @param num
	 *            positions to be '1'.
	 * @param idSize
	 *            max idsize
	 * @return bit mask
	 * @example num=2, idSize=4, returns 1100
	 *
	public static final BigInteger createHighBitMask(int num, int idSize) {
		if (num > idSize || num <= 0 || idSize <= 0)
			throw new RuntimeException("Please don't do this");

		BigInteger toReturn = BigInteger.ZERO;
		for (int it = 0; it < num; it++) {
			toReturn = toReturn.setBit(idSize - it - 1);
		}
		return toReturn;
	}

	**
	 * Sorts predecessors closer to the root at the end. Sorts successors closer
	 * to the root at the beggining
	 * 
	 * @param list
	 * @param myFinger
	 * @return
	 *
	public static final List<Finger> sortList(List<Finger> list, Finger myFinger) {
		if (!list.contains(myFinger)) {
			list.add(myFinger);
		}

		Collections.sort(list);

		for (int i = 0; i < list.size(); i++) {
			if (list.get(i).equals(myFinger)) {
				list.remove(i);
				Collections.rotate(list, list.size() - i);
				break;
			}
		}

		Finger last = null;
		for (int i = 0; i < list.size(); last = list.get(i), i++) {
			if (list.get(i) == last) {
				list.remove(i);
				i--;
			}
		}
		return list;
	}

	public static boolean arrayContains(Object[] array, Object obj) {
		for (Object it : array) {
			if (it == obj) return true;
		}
		return false;
	}

	private static final Utils instance = new Utils();
	public static final NodeComparator nodeComparator = Utils.instance.new NodeComparator();

	public static boolean isBetween(BigInteger id, BigInteger intervalStart,
			BigInteger intervalEnd) {
		if (intervalStart.compareTo(intervalEnd) <= 0)
			return id.compareTo(intervalStart) > 0
					&& id.compareTo(intervalEnd) <= 0;
		// else
		return id.compareTo(intervalStart) > 0 || id.compareTo(intervalEnd) < 0;
	}

	public class NodeComparator implements Comparator<Node> {

		protected NodeComparator() {
			// empty - nothing to do
		}

		@Override
		public int compare(Node o1, Node o2) {
			MyNode obj1 = (MyNode) o1;
			MyNode obj2 = (MyNode) o2;
			return obj1.compareTo(obj2);
		}
	}

	public static Node getLowestNode(Collection<Node> membersNew) {
		if (membersNew.size() == 0) throw new RuntimeException("Oh noes");
		Node toReturn = null;
		long id = Long.MAX_VALUE;
		for (Node it : membersNew) {
			if (id > it.getID()) {
				toReturn = it;
				id = it.getID();
			}
		}
		return toReturn;
	}

	public static Node getLowestNode(HashMap<Node, Mapping> otherVnode) {
		if (otherVnode.size() == 0) return null;
		Node toReturn = null;
		long id = Long.MAX_VALUE;
		for (Entry<Node, Mapping> entry : otherVnode.entrySet()) {
			Node it = entry.getKey();
			if (id > it.getID()) {
				toReturn = it;
				id = it.getID();
			}
		}
		return toReturn;
	}

	public static Finger getMinDistance(Collection<Finger> col, BigInteger to,
			BigInteger maxDistance) {
		BigInteger minDistance = maxDistance;
		Finger found = null;
		for (Finger it : col) {
			BigInteger currentDist = to.subtract(it.id).abs();
			int compareResult = currentDist.compareTo(minDistance);
			if (compareResult < 0) {
				minDistance = currentDist;
				found = it;
			} else if (compareResult == 0) {
				if (found.id.compareTo(it.id) > 0) {
					minDistance = currentDist;
					found = it;
				}
				throw new RuntimeException("This should *very rarely* happen");
			}
		}
		return found;
	}*/

	public static final <T> T removeRandomEl(List<T> list) {
		int randomNumber = CommonState.r.nextInt(list.size());
		return list.remove(randomNumber);
	}

	public static final <T> T getRandomEl(List<T> list) {
		int randomNumber = CommonState.r.nextInt(list.size());
		return list.get(randomNumber);
	}

	public static final <T> T getRandomEl(Collection<T> col) {
		int randomNumber = CommonState.r.nextInt(col.size());
		Iterator<T> it = col.iterator();
		for (int i = 0; i < randomNumber; i++) {
			it.next();
		}
		T toReturn = it.next();
		return toReturn;
	}

	public static final <T> T removeRandomEl(Collection<T> col) {
		int randomNumber = CommonState.r.nextInt(col.size());
		Iterator<T> it = col.iterator();
		for (int i = 0; i < randomNumber; i++) {
			it.next();
		}
		T toReturn = it.next();
		it.remove();
		return toReturn;
	}
	
	public static int getLongestCommonPrefix(BigInteger first,
			BigInteger second, int idLength) {
		BigInteger xorResult = first.xor(second);
		int it = idLength;
		for (; it > 0; it--) {
			if (xorResult.testBit(it - 1)) {
				break;
			}
		}
		return idLength - it;
	}

	public static BigInteger getMinDistance(BigInteger first,
			BigInteger second, BigInteger ringSize) {
		int compareResult = first.compareTo(second);
		if (compareResult < 0) {
			; // ok, first is smaller than second
		} else if (compareResult == 0)
			throw new RuntimeException();
		else {
			BigInteger temp = second;
			second = first;
			first = temp;
		}

		BigInteger distanceLinear = second.subtract(first);
		BigInteger distanceWrap = ringSize.subtract(second).add(first);

		compareResult = distanceLinear.compareTo(distanceWrap);
		if (compareResult < 0)
			return distanceLinear;
		else if (compareResult == 0)
			throw new RuntimeException();
		else
			return distanceWrap;
	}

	public static BigInteger getHalfInterval(BigInteger lower,
			BigInteger higher, BigInteger ringSize) {
		BigInteger two = BigInteger.ONE.add(BigInteger.ONE);
		int compareResult = lower.compareTo(higher);
		if (compareResult < 0)
			return lower.add(higher.subtract(lower).divide(two)).mod(ringSize);
		else if (compareResult == 0)
			throw new RuntimeException();
		else
			return lower.add(
					higher.add(ringSize.subtract(lower)).divide(two)).mod(
					ringSize);
	}

	public static BigInteger getHalfIntervalInverse(BigInteger lower,
			BigInteger higher, BigInteger ringSize) {
		BigInteger two = BigInteger.ONE.add(BigInteger.ONE);
		int compareResult = lower.compareTo(higher);
		if (compareResult < 0)
			return higher.add(ringSize.subtract(higher).add(lower).divide(two))
					.mod(ringSize);
		else if (compareResult == 0)
			throw new RuntimeException();
		else
			return higher.add(lower.subtract(higher).divide(two)).mod(ringSize);
	}
}
