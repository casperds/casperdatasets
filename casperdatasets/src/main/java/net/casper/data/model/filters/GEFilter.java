//	GEFilter.java
//	- Casper Datasets (R) -
//

package net.casper.data.model.filters;

//	Casper imports
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CRowMetaData;
import net.casper.data.model.CDataRow;



/**
 * This filter returns true, if the underlying data is greater than a particular lower bound
 * 
 * @since 1.0
 * @author Jonathan Liang
 * @version $Revision: 111 $ 
 */
public class GEFilter 
	extends CDataFilter
{
	
	//	--- Instance Variables ---

	/** Lower bound, all matches must be > lbound */
	private double lbound = -1;

	/** True, if performing an inclusive match */
	private boolean inclusive = false; 
	
	
	//	--- Constructor(s) ---


	/**
	 * Constructs a "greater-than" numeric filter. 
	 * 
	 * @param columnName
	 * @param columnIndex
	 * @param lbound
	 * @param inclusive 
	 */
	public GEFilter(String columnName, double lbound, boolean inclusive)
		throws CDataGridException
	{
		super(columnName);
		
		this.lbound = lbound;
		this.inclusive = inclusive; 	
	}
	
	//	--- Interface Methods ---
	
	/**
	 * Performs a match for this filter 
	 * 
	 * @return true, if this filter matches
	 * @throws CDataGridException
	 */
	public boolean doesMatch(CDataRow row)
		throws CDataGridException
	{
		//	Check col index initialization 
		checkColumnIndexInitialized();
		
		try
		{
			//	Perform range match on number
			Number numVal = (Number) row.getValue(columnIndex);
			if (numVal == null)
				return false;
			
			double number = numVal.doubleValue();
			
			if (inclusive)
			{
				//	Inclusive match - equality on lbound/ubound is a match 
				if (number >= lbound)
					return true;
			}	
			else
			{
				//	Exclusive match - equality on lbound/ubound is not a match 
				if (number > lbound)
					return true;
			}
		}
		catch (Exception ex)
		{
			throw new CDataGridException("Could not match row value: " + ex.toString(), ex);
		}
		
		return false;
	}


	/**
	 * Returns string representation of this filter 
	 * 	@return string 
	 */
	public String toString()
	{
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("GEFilter :: where ");
		sbuf.append(columnName).append(" (").append(columnIndex).append(") in (");
		sbuf.append(">").append(String.valueOf(lbound)).append("), ");
		if (inclusive)  sbuf.append("(inclusive)");
		else sbuf.append("(exclusive)");
		
		return sbuf.toString();
	}
	
	
}
