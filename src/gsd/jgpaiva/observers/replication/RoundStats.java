package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.protocols.replication.GroupReplication2;
import peersim.core.Network;

public class RoundStats extends ControlImpl {

	public RoundStats(String prefix) {
		super(prefix);

	}

	@Override
	protected boolean executeCycle() {
		if (Network.get(0).getProtocol(this.pid) instanceof GroupReplication2) {
			int merges = GroupReplication2.getRoundMerges();
			int divisions = GroupReplication2.getRoundDivisions();
			int joins = GroupReplication2.getRoundJoins();
			int leaves = GroupReplication2.getRoundLeaves();
			this.println(this.getStep() + ":" + " merges:" + merges + " divisions:" + divisions
					+ " joins:" + joins + " leaves:" + leaves);
		}
		return false;
	}
}
