package systems.intino.dataformats.led.exceptions;

public class StackAllocatorUnderflowException extends RuntimeException {

	public StackAllocatorUnderflowException() {
	}

	public StackAllocatorUnderflowException(String message) {
		super(message);
	}
}
