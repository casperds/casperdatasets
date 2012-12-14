//	InnerJoin.java
//	Casper Datasets (R) 
//

package net.casper.data.model.join;

//	Casper imports
import net.casper.data.model.*;

/**
 * Normal inner join
 * 
 * @since 1.0
 * @author Jonathan H. Liang
 * @version $Revision: 111 $ 
 */
public class InnerJoin 
	implements Join 
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
		throws CDataGridException 
	{
		return join(cA, cB, joinColumns, null);
	}

	
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
		throws CDataGridException 
	{
		
		CRowMetaData aMd = cA.getMetaDefinition();
		CRowMetaData bMd = cB.getMetaDefinition();
		
		
		
		
		
		
		// TODO Auto-generated method stub
		return null;
	}

}
