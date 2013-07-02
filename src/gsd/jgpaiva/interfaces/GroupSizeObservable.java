package gsd.jgpaiva.interfaces;


public interface GroupSizeObservable extends InitializableProtocol, Killable {
	int getGroupSize();

	int getMaxGroupSize();

	int getMinGroupSize();
}
