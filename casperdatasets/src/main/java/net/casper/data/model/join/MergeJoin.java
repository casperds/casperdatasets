//	MergeJoin.java
//	Casper Datasets (R) 
//

package net.casper.data.model.join;

//	Casper imports 
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;


/**
 * 	A merge-join is a very simplistic merge of two datasets. 
 * 	The more sophisticated joins are implemented with InnerJoin and OuterJoin.
 * 
 * @since 1.0
 * @author Jonathan H. Liang
 * @version $Revision: 111 $ 
 */
public class MergeJoin 
	implements Join 
{

	
	/**
	 * 
	 */
	public CDataCacheContainer join(CDataCacheContainer cA, CDataCacheContainer cB, String[] joinColumns)
		throws CDataGridException 
	{
		// TODO Auto-generated method stub
		return null;
	}

	public CDataCacheContainer join(CDataCacheContainer cA, CDataCacheContainer cB, String[] joinColumns, String[] selectedColumns) 
		throws CDataGridException 
	{
		// TODO Auto-generated method stub
		return null;
	}

}
