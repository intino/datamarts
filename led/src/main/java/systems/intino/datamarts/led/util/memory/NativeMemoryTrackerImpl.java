package systems.intino.datamarts.led.util.memory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

class NativeMemoryTrackerImpl implements NativeMemoryTracker {

	private final Queue<AllocationInfo> liveAllocations;

	public NativeMemoryTrackerImpl() {
		liveAllocations = new ConcurrentLinkedQueue<>();
	}

	void track(AllocationInfo allocationInfo) {
		removeGarbageCollectedAllocations();
		liveAllocations.add(allocationInfo);
	}

	@Override
	public List<AllocationInfo> getLiveAllocations() {
		removeGarbageCollectedAllocations();
		return liveAllocations.stream().collect(Collectors.toUnmodifiableList());
	}

	@Override
	public int numLiveAllocations() {
		removeGarbageCollectedAllocations();
		return liveAllocations.size();
	}

	@Override
	public long totalAllocationSize() {
		return liveAllocations.stream().mapToLong(AllocationInfo::size).sum();
	}

	private void removeGarbageCollectedAllocations() {
		liveAllocations.removeIf(AllocationInfo::isGarbageCollected);
	}

}
