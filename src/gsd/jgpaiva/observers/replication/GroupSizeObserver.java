package gsd.jgpaiva.observers.replication;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.GroupSizeObservable;
import gsd.jgpaiva.utils.IncrementalFreq;
import gsd.jgpaiva.utils.IncrementalStats;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;

public class GroupSizeObserver extends ControlImpl {
	public GroupSizeObserver(String prefix) {
		super(prefix);
	}

	@Override
	public boolean executeCycle() {
		IncrementalFreq freq = new IncrementalFreq();
		IncrementalStats stats = new IncrementalStats();
		int minGroupSize;
		int maxGroupSize;

		Node currentNode = Network.get(0);
		GroupSizeObservable current = (GroupSizeObservable) currentNode.getProtocol(this.pid);
		minGroupSize = current.getMinGroupSize();
		maxGroupSize = current.getMaxGroupSize();
		int notInitialized = 0;

		for (int i = 0; i < Network.size(); i++) {
			currentNode = Network.get(i);
			current = (GroupSizeObservable) currentNode.getProtocol(this.pid);
			if (!current.isUp()) {
				continue;
			}
			if (!current.isInitialized()) {
				notInitialized++;
				this.debugPrintln(CommonState.getTime() + " " + this.name + " " + currentNode
						+ " is not initialized");
				continue;
			}
			int currentGS = current.getGroupSize();

			freq.add(currentGS);
			stats.add(currentGS);

			if (currentGS < minGroupSize) {
				this.debugPrintln(CommonState.getTime() + " " + this.name + " " + currentNode
						+ " is bellow minSize:" + currentGS + " vs " + minGroupSize);
			}
			if (currentGS > maxGroupSize) {
				this.debugPrintln(CommonState.getTime() + " " + this.name + " " + currentNode
						+ " is above maxSize:" + currentGS + " vs " + maxGroupSize);
			}
		}
		freq.normalize();
		this.println(CommonState.getTime() + " " + this.name + " freq:" + freq + " stats:"
				+ stats.toStringLong() + " notInitialized:" + notInitialized);
		return false;
	}
}
