//	CDataGridException.java
//	- Casper Datasets (R) -
//

package net.casper.data.model; 

//	Java imports
import java.io.*;



/**
 *	Represents the generic exception for all data-grid based functionality 
 *
 *	@since 1.0
 *	@author Jonathan Liang
 *  @version $Revision: 111 $
 */
public class CDataGridException
	extends Exception 
{
	
	//	--- Static Variables ---
	
	/**	Required for serializable */
	private static final long serialVersionUID = 1L;


	//	--- Constructor(s) ---
	
	/**
	 *	Exceptions must be thrown with some reason !!
	 */
	private CDataGridException() 
	{
	}
	
	/**
	 *	Throw exception with message / reason 
	 *	@param message
	 */
	public CDataGridException(String message) 
	{
		super(message);
	}	
	
	/**
	 *	Throw exception with previous (chained) error) 
	 *	@param message
	 *	@param previous
	 */
	public CDataGridException(String message, Throwable previous) 
	{
    	super(message,previous);
	}	

	/**
	 *	Throw exception with previous (chained) error). 
	 *	@param previous
	 */
	public CDataGridException(Throwable previous) 
	{
    	super(previous);
	}	


	//	--- Instance Methods ---
	

	/**
	 *	Return cause 
	 *	@return cause 
	 */
	public Throwable getPreviousError()
	{
		return getCause();
	}


	//	--- Static Methods --- 

	/**
	 *	Get string representation of trace
	 *	@param ex
	 *	@return string representation of stack trace 
	 */
	public static String getStackTraceAsString(Throwable thrown)
	{
		StringWriter buffer = new StringWriter();
		PrintWriter ppw = new PrintWriter(buffer);
		
		//	Move deeper into the stack 
		int depth = 0; 
		Throwable ex = thrown;
		while (ex != null) 
		{
			if (depth > 0)
				ppw.write("Caused by: \n");
			
			//	Print stack trace 
			ex.printStackTrace(ppw);
			
			//	Get cause of the current error 
			ex = ex.getCause();
			depth++;
		}
		
		
		ppw.flush();
		ppw.close();
		
		return buffer.toString();
	}

	
}
