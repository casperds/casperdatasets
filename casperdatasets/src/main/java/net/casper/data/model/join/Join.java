//	Join.java
//	Casper Datasets (R)
//

package net.casper.data.model.join;

//	Java imports
import java.util.*;

//	Casper datasets 
import net.casper.data.model.*;


/**
 * Interface for various join algorithms.
 * Various join strategies should be implemented via this interface
 * (Inner join, Outer join, Merge, etc)
 * 
 * @since 1.0
 * @author Jonathan Liang
 * @version $Revision: 111 $ 
 */
public interface Join 
{

	
	/**
	 * Join two cache containers together.
	 * 
	 * @param cA
	 * @param cB
	 * @param joinColumns
	 * @return
	 * @throws CDataGridException
	 */
	public CDataCacheContainer join(CDataCacheContainer cA, CDataCacheContainer cB, String[] joinColumns)
		throws CDataGridException;
	
	
	/**
	 * Join two cache containers together, with selected columns
	 * 
	 * @param cA
	 * @param cB
	 * @param joinColumns
	 * @return
	 * @throws CDataGridException
	 */
	public CDataCacheContainer join(CDataCacheContainer cA, CDataCacheContainer cB, String[] joinColumns, String[] selectedColumns)
		throws CDataGridException;
	
	
}
