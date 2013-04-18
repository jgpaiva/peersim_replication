package gsd.jgpaiva.interfaces;

import gsd.jgpaiva.utils.IntCounter;
import gsd.jgpaiva.utils.LongCounter;
import gsd.jgpaiva.utils.Pair;

import java.util.Map;

public interface WeightedMessageCostObservable extends InitializableProtocol{
	public Map<Class<?>, Pair<IntCounter, LongCounter>> getWeightedSentMessages();

	public void resetSentMessages();
}
