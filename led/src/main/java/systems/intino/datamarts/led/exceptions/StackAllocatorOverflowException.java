package systems.intino.datamarts.led.exceptions;

public class StackAllocatorOverflowException extends RuntimeException {

	public StackAllocatorOverflowException() {
	}

	public StackAllocatorOverflowException(String message) {
		super(message);
	}
}
