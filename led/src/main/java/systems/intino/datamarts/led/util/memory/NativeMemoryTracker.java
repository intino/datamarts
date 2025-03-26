package systems.intino.datamarts.led.util.memory;

import java.util.List;

public interface NativeMemoryTracker {

    List<AllocationInfo> getLiveAllocations();

    int numLiveAllocations();

    long totalAllocationSize();

    default float totalAllocationSizeMB() {
        return totalAllocationSize() / 1024.0f / 1024.0f;
    }

    default AllocationInfo find(long address) {
        return getLiveAllocations().stream()
                .filter(allocationInfo -> allocationInfo.address().get() == address)
                .findAny().orElse(null);
    }
}
