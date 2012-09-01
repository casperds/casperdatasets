package net.casper.io.file.in;

import java.io.IOException;

/**
 * Exception that signals that the file type is not supported.
 * 
 * @see CBuildFromFile
 * 
 * @author Oliver Mannion
 * @version 1.0
 * 
 */
public class UnsupportedFileTypeException extends IOException {

	/**
	 * Serialization ID.
	 */
	private static final long serialVersionUID = 1526394980816589208L;

	/**
	 * Throw exception with message / reason.
	 * 
	 * @param message
	 *            The detail message (which is saved for later retrieval by the
	 *            {@link #getMessage()} method)
	 */
	public UnsupportedFileTypeException(String message) {
		super(message);
	}

	/**
	 * Throw exception with previous (chained) exception.
	 * 
	 * <p>
	 * Note that the detail message associated with {@code cause} is <i>not</i>
	 * automatically incorporated into this exception's detail message.
	 * 
	 * @param message
	 *            The detail message (which is saved for later retrieval by the
	 *            {@link #getMessage()} method)
	 * 
	 * @param cause
	 *            The cause (which is saved for later retrieval by the
	 *            {@link #getCause()} method). (A null value is permitted, and
	 *            indicates that the cause is nonexistent or unknown.)
	 */
	public UnsupportedFileTypeException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructs an exception with the specified cause and a detail message of
	 * {@code (cause==null ? null : cause.toString())} (which typically contains
	 * the class and detail message of {@code cause}). This constructor is
	 * useful for IO exceptions that are little more than wrappers for other
	 * throwables.
	 * 
	 * @param cause
	 *            The cause (which is saved for later retrieval by the
	 *            {@link #getCause()} method). (A null value is permitted, and
	 *            indicates that the cause is nonexistent or unknown.)
	 * 
	 * @since 1.6
	 */
	public UnsupportedFileTypeException(Throwable cause) {
		super(cause);
	}

}