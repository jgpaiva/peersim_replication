package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.protocols.replication.GroupReplication;
import peersim.core.Network;

public class RoundStats extends ControlImpl {

	public RoundStats(String prefix) {
		super(prefix);

	}

	@Override
	protected boolean executeCycle() {
		if (Network.get(0).getProtocol(this.pid) instanceof GroupReplication) {
			int merges = GroupReplication.getRoundMerges();
			int divisions = GroupReplication.getRoundDivisions();
			int joins = GroupReplication.getRoundJoins();
			int leaves = GroupReplication.getRoundLeaves();
			this.println(this.getStep() + ":" + " merges:" + merges + " divisions:" + divisions
					+ " joins:" + joins + " leaves:" + leaves);
		}
		return false;
	}
}
