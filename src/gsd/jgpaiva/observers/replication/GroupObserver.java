package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.GroupSizeObservable;
import gsd.jgpaiva.protocols.replication.Group;
import gsd.jgpaiva.utils.IncrementalFreq;
import gsd.jgpaiva.utils.IncrementalStats;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

public class GroupObserver extends ControlImpl {
	public GroupObserver(String prefix) {
		super(prefix);
	}

	@Override
	public boolean executeCycle() {
		for (Group it : Group.groups) {
			this.println(this.getStep() + " " + it);
		}

		return false;
	}
}
