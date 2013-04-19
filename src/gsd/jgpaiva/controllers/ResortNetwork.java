package gsd.jgpaiva.controllers;

import gsd.jgpaiva.protocols.replication.GRUtils;
import gsd.jgpaiva.protocols.replication.Group;
import gsd.jgpaiva.protocols.replication.GroupReplication2;
import gsd.jgpaiva.protocols.replication.GroupReplication2.Mode;
import gsd.jgpaiva.utils.Pair;

import java.util.List;

import peersim.config.Configuration;
import peersim.core.Network;
import peersim.core.Node;

public class ResortNetwork extends ControlImpl {
	private static final String PAR_MIN_TIME = "mintime";

	private final int minTime;

	public ResortNetwork(String prefix) {
		super(prefix);
		this.minTime = Configuration.getInt(prefix + "." + ResortNetwork.PAR_MIN_TIME);

	}

	@Override
	protected boolean executeCycle() {
		if (!(Network.get(0).getProtocol(this.pid) instanceof GroupReplication2)) {
			this.println("Oh noes, this is not a GroupReplication2 kind of protocol!");
			return true;
		}

		if (this.getStep() == this.minTime) {
			reliableNodesToLargerGroups();
			setMode(GroupReplication2.Mode.LNLB_PREEMPTIVE);
		}

		return false;
	}

	private void setMode(Mode newMode) {
		((GroupReplication2) Network.get(0).getProtocol(this.pid)).setMode(newMode);
	}

	private void reliableNodesToLargerGroups() {
		List<Pair<Node, Integer>> deathTimes = GRUtils.listNodeDeaths(Group.groups);
		List<Pair<Group, Integer>> groupKeys = GRUtils.listGroupKeys(Group.groups);

		int counter = 0;
		for (Pair<Group, Integer> it : groupKeys) {
			Group grp = it.fst;

			int size = grp.size();
			grp.getFinger().clear();
			for (int it2 = 0; it2 < size; it2++) {
				Pair<Node, Integer> n = deathTimes.get(counter++);
				grp.getFinger().add(n.fst);
			}
			grp.updateMembers();
		}
	}
}
