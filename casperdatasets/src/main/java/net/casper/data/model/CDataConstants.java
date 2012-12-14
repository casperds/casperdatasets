//	CDataConstants.java 
//	- Casper Datasets (R) -
//

package net.casper.data.model;


/**
 * Constants used internally by this data caching framework.
 * 
 * @since 1.0
 * @author Jonathan Liang
 * @version $Revision: 111 $ 
 */
public final class CDataConstants 
{

	//
	//	--- Static Variables --- 
	//
	
	/** 
	 * 	Meta-data key value:
	 * 	Stored into cache store as a key-value pair, will need a constant to 
	 * 	define / distinguish this particular entry in cache. 
	 */
	public static final String META_DATA = "META_DATA";

	/** 
	 * 	Indice(s) key value:
	 *	Like meta-data, store as a key-value pair in underlying cache store. 
	 */
	public static final String INDICES = "INDICES";
	
	
	//
	//	--- Constructor(s) --- 
	//
	
	/**
	 *	Disallow private instantiation.  	
	 */
	private CDataConstants()
	{
	}

	
	
	
}
