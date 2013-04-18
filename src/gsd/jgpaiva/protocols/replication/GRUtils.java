package gsd.jgpaiva.protocols.replication;

import gsd.jgpaiva.utils.Pair;

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
	static int getDeathTime(Node n) {
		return GroupReplication2.getProtocol(n).deathTime;
	}

	static int getGroupDeath(Group g) {
		ArrayList<Integer> deathTimes = new ArrayList<Integer>();
		for (Node i : g.getFinger()) {
			deathTimes.add(getDeathTime(i));
		}
		Collections.sort(deathTimes);

		int numToMerge = g.size() - GroupReplication2.minReplication; // if(g.size
																		// <
		// minReplication)

		Iterator<Integer> iter = deathTimes.iterator();
		int groupDeath = iter.next();
		for (int it = 0; it < numToMerge; it++) {
			groupDeath = iter.next();
		}
		return groupDeath;
	}

	static List<Pair<Group, Integer>> getGroupDeathList() {
		List<Pair<Group, Integer>> lst = new ArrayList<Pair<Group, Integer>>();
		for (Group i : Group.groups) {
			lst.add(new Pair<Group, Integer>(i, getGroupDeath(i)));
		}

		Collections.sort(lst, new Comparator<Pair<Group, Integer>>() {
			@Override
			public int compare(Pair<Group, Integer> o1, Pair<Group, Integer> o2) {
				return -o2.snd + o1.snd;
			}
		});
		return lst;
	}

	static List<Pair<Group, Double>> getGroupAverageLoadList(Collection<Group> c) {
		List<Pair<Group, Double>> lst = new ArrayList<Pair<Group, Double>>();
		for (Group i : c) {
			lst.add(new Pair<Group, Double>(i, calcAvgLoad(i)));
		}

		Collections.sort(lst, new Comparator<Pair<Group, Double>>() {
			@Override
			public int compare(Pair<Group, Double> o1, Pair<Group, Double> o2) {
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

	public static List<Group> getSmallestList() {
		double smallSize = Integer.MAX_VALUE;
		List<Group> toReturn = new ArrayList<Group>();

		for (Group it : Group.groups) {
			if (it.size() < smallSize) {
				toReturn.clear();
				toReturn.add(it);
				smallSize = it.size();
			} else if (it.size() == smallSize) {
				toReturn.add(it);
			}
		}
		assert (toReturn.size() > 0) : Group.groups;
		return toReturn;
	}

	public static List<Group> getLargestList() {
		double largeSize = 0;
		List<Group> toReturn = new ArrayList<Group>();

		for (Group it : Group.groups) {
			if (it.size() > largeSize) {
				toReturn.clear();
				toReturn.add(it);
				largeSize = it.size();
			} else if (it.size() == largeSize) {
				toReturn.add(it);
			}
		}
		assert (toReturn.size() > 0) : Group.groups;
		return toReturn;
	}

	static Group getShortestDeath(Collection<Group> lst) {
		Node node = null;
		Group group = null;
		int minTime = 0;

		for (Group g : lst) {
			for (Node n : g.getFinger()) {
				if (node == null) {
					node = n;
					group = g;
					minTime = getDeathTime(n);
				} else if (getDeathTime(n) < minTime) {
					node = n;
					group = g;
					minTime = getDeathTime(n);
				}
			}
		}
		return group;
	}

	static Group getShortestGroupDeath(Collection<Group> lst) {
		Group group = null;
		int minTime = 0;

		for (Group g : lst) {
			if (group == null) {
				group = g;
				minTime = getGroupDeath(g);
			} else if (getGroupDeath(g) < minTime) {
				group = g;
				minTime = getGroupDeath(g);
			}
		}
		return group;
	}

	static Group getMostAverageLoaded(Collection<Group> c) {
		double maxLoad = 0;
		Group toReturn = null;

		for (Group it : c) {
			double load = calcAvgLoad(it);
			if (toReturn == null || load > maxLoad) {
				maxLoad = load;
				toReturn = it;
			}
		}
		return toReturn;
	}

	public static double calcAvgLoad(Group g) {
		return ((double) g.load()) / g.size();
	}

	public static Group getMostLoaded(List<Group> smallestList) {
		double maxLoad = 0;
		Group toReturn = null;

		for (Group it : getSmallestList()) {
			double load = it.load();
			if (load > maxLoad) {
				maxLoad = load;
				toReturn = it;
			}
		}
		return toReturn;
	}

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
			if (retVal.size() > (initialSize * perc))
				break;
			retVal.add(i.fst);
		}
		return retVal;
	}

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

	public static boolean isAboveAverage(GroupReplication2 n, HashSet<Group> groups, double percent) {
		int count = 0;
		int total = 0;
		int deathTime = n.deathTime;
		for (Group it : groups) {
			for (Node j : it.getFinger()) {
				if (getDeathTime(j) > deathTime)
					count++;
				total++;
			}
		}
		return ((double) count) / total > percent;
	}

	public static Node getMinDeathTime(Collection<Node> c) {
		int max = 0;
		Node n = null;
		for (Node it : c) {
			int dt = getDeathTime(it);
			if (n == null || dt < max) {
				max = dt;
				n = it;
			}
		}
		return n;
	}
}
