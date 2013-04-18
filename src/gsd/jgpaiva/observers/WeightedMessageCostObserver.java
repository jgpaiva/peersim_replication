package gsd.jgpaiva.observers;

import gsd.jgpaiva.controllers.ControlImpl;
import gsd.jgpaiva.interfaces.WeightedMessageCostObservable;
import gsd.jgpaiva.utils.GenericIncrementalFreq;
import gsd.jgpaiva.utils.IntCounter;
import gsd.jgpaiva.utils.LongCounter;
import gsd.jgpaiva.utils.Pair;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;

import peersim.core.Network;

public class WeightedMessageCostObserver extends ControlImpl {
	private PrintStream ps2;
	private PrintStream ps3;

	public WeightedMessageCostObserver(String prefix) {
		super(prefix);

		try {
			this.ps2 = new PrintStream(this.filename + ".out");
		} catch (FileNotFoundException e) {
			this.ps2 = System.out;
			System.err.println(this.name + ": could not set write to file:" + this.filename
					+ " Writing to system.out instead.");
		}
		try {
			this.ps3 = new PrintStream(this.filename + ".weighted");
		} catch (FileNotFoundException e) {
			this.ps3 = System.out;
			System.err.println(this.name + ": could not set write to file:" + this.filename
					+ " Writing to system.out instead.");
		}
	}

	@Override
	public boolean executeCycle() {

		long totalMessagesSent = 0;
		Map<Class<?>, Pair<IntCounter, LongCounter>> messagesSent = null;
		int totalNodes = 0;
		double averageMessagesSent;
		GenericIncrementalFreq<Class<?>> messageDistribution = new GenericIncrementalFreq<Class<?>>();
		GenericIncrementalFreq<Class<?>> weightedMessageDistribution = new GenericIncrementalFreq<Class<?>>();

		for (int i = 0; i < Network.size(); i++) {
			WeightedMessageCostObservable mo = (WeightedMessageCostObservable) Network.get(i)
					.getProtocol(this.pid);
			if (mo.isInitialized()) {
				totalNodes++;
				messagesSent = mo.getWeightedSentMessages();
				for (Entry<Class<?>, Pair<IntCounter, LongCounter>> it : messagesSent.entrySet()) {
					totalMessagesSent += it.getValue().fst.value;
					messageDistribution.add(it.getKey(), it.getValue().fst.value);
					if (it.getValue().snd.value > 0) {
						weightedMessageDistribution.add(it.getKey(), it.getValue().snd.value);
					}
				}
				mo.resetSentMessages();
			}
		}

		averageMessagesSent = totalMessagesSent;
		averageMessagesSent = averageMessagesSent / totalNodes;

		try {
			this.processDataAndPrint(totalMessagesSent, averageMessagesSent, messageDistribution,
					weightedMessageDistribution);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return false;
	}

	private void processDataAndPrint(long total, double average,
			GenericIncrementalFreq<Class<?>> dist, GenericIncrementalFreq<Class<?>> weightedDist)
			throws Exception {
		this.println(this.getStep() + " total messages: " + total + " average per node: " + average);
		this.ps2.println(this.getStep() + ": " + dist.toString());
		this.ps3.println(this.getStep() + ": " + weightedDist.toString());
	}
}
