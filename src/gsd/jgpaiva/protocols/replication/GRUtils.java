package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.utils.Pair;
import gsd.jgpaiva.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

import peersim.core.Node;

public class GRUtils {

	/***************** DEATH TIME related functions ******************/

	public static Group getNextGroupDeath(Collection<Group> lst) {
		Node node = null;
		Group group = null;
		int minTime = 0;

		for (Group g : lst) {
			for (Node n : g.getFinger()) {
				if (node == null) {
					node = n;
					group = g;
					minTime = getNodeDeath(n);
				} else if (getNodeDeath(n) < minTime) {
					node = n;
					group = g;
					minTime = getNodeDeath(n);
				}
			}
		}
		return group;
	}

	public static int getNodeDeath(Node n) {
		return GroupReplication.getProtocol(n).deathTime;
	}

	public static int getGroupDeath(Group g) {
		ArrayList<Integer> deathTimes = new ArrayList<Integer>();
		for (Node i : g.getFinger()) {
			deathTimes.add(getNodeDeath(i));
		}
		Collections.sort(deathTimes);

		int numToMerge = g.size() - GroupReplication.minReplication;
		// if(g.size < minReplication)

		Iterator<Integer> iter = deathTimes.iterator();
		int groupDeath = iter.next();
		for (int it = 0; it < numToMerge; it++) {
			groupDeath = iter.next();
		}
		return groupDeath;
	}

	public static List<Pair<Group, Integer>> listGroupDeaths(Collection<Group> c) {
		List<Pair<Group, Integer>> lst = new ArrayList<Pair<Group, Integer>>();
		for (Group i : c) {
			lst.add(new Pair<Group, Integer>(i, getGroupDeath(i)));
		}

		Collections.sort(lst, new ComparePairsSortByLargest<Group>());
		return lst;
	}

	private static class ComparePairsSortByLargest<T> implements Comparator<Pair<T, Integer>> {
		@Override
		public int compare(Pair<T, Integer> o1, Pair<T, Integer> o2) {
			return -(o2.snd - o1.snd);
		}
	}

	public static Node getMinDeath(Collection<Node> c) {
		int max = 0;
		Node n = null;
		for (Node it : c) {
			int dt = getNodeDeath(it);
			if (n == null || dt < max) {
				max = dt;
				n = it;
			}
		}
		return n;
	}

	/**
	 * Get list of node deaths, sorted by largest (most distant death) to
	 * smallest.
	 * 
	 * @param groups
	 * @return
	 */
	public static List<Pair<Node, Integer>> listNodeDeaths(HashSet<Group> groups) {
		List<Pair<Node, Integer>> retVal = new ArrayList<Pair<Node, Integer>>();
		for (Group i : groups) {
			for (Node j : i.getFinger()) {
				retVal.add(new Pair<Node, Integer>(j, getNodeDeath(j)));
			}
		}
		Collections.sort(retVal, new ComparePairsSortByLargest<Node>());
		return retVal;
	}

	public static boolean isReliable(Node n, TreeSet<Node> activenodes, double percent) {
		int total = activenodes.size();

		int countMoreReliable = activenodes.tailSet(n, false).size();

		double moreReliablePercent = ((double) countMoreReliable) / total;
		return moreReliablePercent <= percent;
	}

	/***************** LOAD related functions ******************/

	public static List<Pair<Group, Double>> listGroupAverageLoads(Collection<Group> c) {
		List<Pair<Group, Double>> lst = new ArrayList<Pair<Group, Double>>();
		for (Group i : c) {
			lst.add(new Pair<Group, Double>(i, getAvgGroupLoad(i)));
		}

		Collections.sort(lst, new Comparator<Pair<Group, Double>>() {
			@Override
			public int compare(Pair<Group, Double> o1, Pair<Group, Double> o2) {
				// must test, since must return an int
				if (o2.snd > o1.snd)
					return -1;
				else if (o2.snd < o1.snd)
					return 1;
				else
					return 0;
			}
		});
		return lst;
	}

	public static Group getMostAverageLoaded(Collection<Group> c) {
		double maxLoad = 0;
		Group toReturn = null;

		for (Group it : c) {
			double load = getAvgGroupLoad(it);
			if (toReturn == null || load > maxLoad) {
				maxLoad = load;
				toReturn = it;
			}
		}
		return toReturn;
	}

	public static Group getMostLoaded(Collection<Group> smallestList) {
		double maxLoad = 0;
		Group toReturn = null;

		for (Group it : smallestList) {
			double load = it.load();
			if (load > maxLoad) {
				maxLoad = load;
				toReturn = it;
			}
		}
		return toReturn;
	}

	public static double getAvgGroupLoad(Group g) {
		return ((double) g.load()) / g.size();
	}

	/***************** SIZE related functions ******************/

	public static List<Group> listSmallest(Collection<Group> c) {
		double smallSize = Integer.MAX_VALUE;
		List<Group> toReturn = new ArrayList<Group>();

		for (Group it : c) {
			if (it.size() < smallSize) {
				toReturn.clear();
				toReturn.add(it);
				smallSize = it.size();
			} else if (it.size() == smallSize) {
				toReturn.add(it);
			}
		}
		assert (toReturn.size() > 0) : c;
		return toReturn;
	}

	public static List<Group> listLargest(Collection<Group> c) {
		double largeSize = 0;
		List<Group> toReturn = new ArrayList<Group>();

		for (Group it : c) {
			if (it.size() > largeSize) {
				toReturn.clear();
				toReturn.add(it);
				largeSize = it.size();
			} else if (it.size() == largeSize) {
				toReturn.add(it);
			}
		}
		assert (toReturn.size() > 0) : c;
		return toReturn;
	}

	/***************** KEYS related functions ******************/

	public static Group getMostKeys(Collection<Group> c) {
		Group maxGrp = null;
		for (Group i : c) {
			if (maxGrp == null || maxGrp.keys() < i.keys())
				maxGrp = i;
		}
		return maxGrp;
	}

	public static Group getFewestKeys(Collection<Group> c) {
		Group minGrp = null;
		for (Group i : c) {
			if (minGrp == null || minGrp.keys() > i.keys())
				minGrp = i;
		}
		return minGrp;
	}

	public static Collection<Group> filterSingleKey(Collection<Group> c) {
		Collection<Group> retVal = new ArrayList<Group>();
		for (Group i : c) {
			if (i.keys() > 1 || i.size() < GroupReplication.maxReplication) {
				retVal.add(i);
			}
		}
		return retVal;
	}

	/**
	 * get a list of groups, sorted by descending number of keys.
	 * 
	 * @param groups
	 * @return
	 */
	public static List<Pair<Group, Integer>> listGroupKeys(Collection<Group> groups) {
		List<Pair<Group, Integer>> retVal = new ArrayList<Pair<Group, Integer>>();
		for (Group it : groups) {
			retVal.add(new Pair<Group, Integer>(it, it.keys()));
		}
		Collections.sort(retVal, new ComparePairsSortByLargest<Group>());
		return retVal;
	}

	/************************ MISC functions ****************************/

	/**
	 * slice a list, considering only the first percentage of items
	 * 
	 * @param c
	 * @param perc
	 * @return
	 */
	public static <X, Y> Collection<X> slicePercentage(Collection<Pair<X, Y>> c, double perc) {
		List<X> retVal = new ArrayList<X>();
		if (c.size() < 3) {
			for (Pair<X, Y> i : c) {
				retVal.add(i.fst);
			}
			return retVal;
		}

		int initialSize = c.size();
		for (Pair<X, Y> i : c) {
			if (retVal.size() >= (initialSize * perc))
				break;
			retVal.add(i.fst);
		}
		return retVal;
	}

	/**
	 * slice a list, considering everything but the first percentage of items
	 * 
	 * @param c
	 * @param perc
	 * @return
	 */
	public static <X, Y> Collection<X> sliceInversePercentage(Collection<Pair<X, Y>> c, double perc) {
		List<X> retVal = new ArrayList<X>();

		int initialSize = c.size();
		int count = 0;
		for (Pair<X, Y> i : c) {
			if (count++ <= (initialSize * perc))
				continue;
			retVal.add(i.fst);
		}

		if (retVal.size() == 0 && c.size() > 0) {
			retVal.add(Utils.getRandomEl(c).fst);
		}
		return retVal;
	}

	/**
	 * remove items from list which are bellow the average
	 * 
	 * @param c
	 * @param perc
	 * @return
	 */
	public static <T> List<T> listAboveAverage(List<Pair<T, Double>> c) {
		double total = 0;
		for (Pair<T, Double> it : c) {
			total += it.snd;
		}
		double avg = total / c.size();
		List<T> retVal = new ArrayList<T>();
		for (Pair<T, Double> it : c) {
			if (it.snd >= avg)
				retVal.add(it.fst);
		}

		if (retVal.size() == 0 && c.size() > 0) {
			retVal.add(Utils.getRandomEl(c).fst);
		}
		return retVal;
	}

	/************************ Key array pointer related functions ****************************/

	/**
	 * Gets the number of keys corresponding to an interval.
	 * 
	 * @param totalKeys
	 * @param keyBott
	 *            bottom of key interval. if == -1, then match all keys
	 * @param keyCeil
	 * @return
	 */
	public static int calculateIntervalSize(int totalKeys, int keyBott, int keyCeil) {
		if (keyBott == -1)
			if (keyCeil == -1) {
				return totalKeys;
			} else {
				throw new RuntimeException("Should never happen: " + keyCeil);
			}
		if (keyCeil == -1 && keyBott != -1) {
			throw new RuntimeException("Should never happen: " + keyBott);
		}

		if (keyBott == keyCeil) {
			return 0;
		}
		if (keyBott < keyCeil) {
			return keyCeil - keyBott;
		} else {
			return (keyCeil + 1) + (totalKeys - keyBott - 1);
		}
	}

	/**
	 * Gets a circular iterator for an array of keys, starting on a given
	 * position and iterating forward in the array (i.e. from the bottom of the
	 * interval to the ceiling)
	 * 
	 * @param toIter
	 * @param startPos
	 * @return
	 */
	public static Iterator<Integer> circularIterForward(final Object[] toIter, final int startPos) {
		return new Iterator<Integer>() {
			int pos = startPos;

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public Integer next() {
				++pos;
				if (!(pos < toIter.length)) {
					pos = 0;
				}
				return pos;
			}

			@Override
			public void remove() {
				throw new RuntimeException("Not implemented");
			}
		};
	}

	public static Iterator<Integer> circularIterBackward(final Object[] toIter, final int startPos) {
		return new Iterator<Integer>() {
			int pos = startPos;

			@Override
			public boolean hasNext() {
				return true;
			}

			@Override
			public Integer next() {
				int retVal = pos;
				--pos;
				if (pos < 0) {
					pos = toIter.length - 1;
				}
				return retVal;
			}

			@Override
			public void remove() {
				throw new RuntimeException("Not implemented");
			}
		};
	}
}
