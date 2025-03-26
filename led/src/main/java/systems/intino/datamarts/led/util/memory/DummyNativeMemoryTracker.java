package systems.intino.datamarts.led.util.memory;

import java.util.Collections;
import java.util.List;

class DummyNativeMemoryTracker implements NativeMemoryTracker {

	@Override
	public List<AllocationInfo> getLiveAllocations() {
		return Collections.emptyList();
	}

	void track() {

	}

	@Override
	public int numLiveAllocations() {
		return 0;
	}

	@Override
	public long totalAllocationSize() {
		return 0;
	}
}
