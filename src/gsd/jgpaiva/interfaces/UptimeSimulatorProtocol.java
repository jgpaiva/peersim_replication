package gsd.jgpaiva.interfaces;

import gsd.jgpaiva.utils.Pair;

import java.util.Collection;

import peersim.core.Node;

public interface UptimeSimulatorProtocol extends Killable {

	void startup(Node myNode, Collection<Pair<Node, Integer>> availabilityList, int deathTime,
			int currentTime);

	void kill(Collection<Pair<Node, Integer>> availabilityList);

	void startSim(Node myNode);
}
