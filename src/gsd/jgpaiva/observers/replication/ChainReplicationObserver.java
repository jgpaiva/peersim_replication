package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.ChainReplProtocol;
import gsd.jgpaiva.interfaces.MonitorableProtocol;
import peersim.core.Network;
import peersim.util.IncrementalFreq;

public class ChainReplicationObserver extends ControlImpl {

	public ChainReplicationObserver(String prefix) {
		super(prefix);
	}

	@Override
	protected boolean executeCycle() {
		IncrementalFreq stats = new IncrementalFreq();
		int count = 0;
		for (int it = 0; it < Network.size(); it++) {
			ChainReplProtocol proto = (ChainReplProtocol) Network.get(it).getProtocol(this.pid);
			if (!proto.isUp()) {
				continue;
			}
			count++;
			stats.add(proto.getAndClearChainMessages());
		}
		this.println(stats.toString() + "\ntotal nodes: " + count);
		return false;
	}
}
