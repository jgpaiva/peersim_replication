package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.KeyStorageProtocol;
import gsd.jgpaiva.interfaces.Killable;
import gsd.jgpaiva.utils.IncrementalFreq;
import peersim.core.Network;
import peersim.core.Node;


public class KeyObserver extends ControlImpl {

	public KeyObserver(String prefix) {
		super(prefix);

	}

	@Override
	protected boolean executeCycle() {
		IncrementalFreq freq = new IncrementalFreq();
		IncrementalFreq freq2 = new IncrementalFreq();
		for (int it = 0; it < Network.size(); it++) {
			Node n = Network.get(it);

			KeyStorageProtocol proto = ((KeyStorageProtocol) n.getProtocol(this.pid));

			if (((Killable) proto).isUp()) {
				freq.add(proto.getKeys());

				double ratio = (((double) proto.getKeys()) / (proto.getReplicationDegree()));
				freq2.add((long) (ratio * 100));
			}
		}
		this.println(this.getStep() + ": " + freq + "\n" + freq2);
		return false;
	}
}
