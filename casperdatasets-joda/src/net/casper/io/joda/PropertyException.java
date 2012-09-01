package net.casper.io.joda;

/**
 * Exception when working with bean property.
 * 
 * @author Oliver Mannion
 * @version $Revision: 95 $
 */
public class PropertyException extends Exception {

	/**
	 * Serialization ID.
	 */
	private static final long serialVersionUID = 5913871867615645239L;

	public PropertyException(String message) {
		super(message);
	}

	public PropertyException(Throwable cause) {
		super(cause);
	}

	public PropertyException(String message, Throwable cause) {
		super(message, cause);
	}
}
