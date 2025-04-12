package systems.intino.dataformats.led.exceptions;

public class StackAllocatorOverflowException extends RuntimeException {

	public StackAllocatorOverflowException() {
	}

	public StackAllocatorOverflowException(String message) {
		super(message);
	}
}
