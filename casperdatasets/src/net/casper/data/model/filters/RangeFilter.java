//	RangeFilter.java 
//	- Casper Datasets (R) -
//

package net.casper.data.model.filters;


//	Casper imports 
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRow;
import net.casper.data.model.CRowMetaData;


/**
 * 	A range filter will check an input value's numerical representation against 
 * 	upper and lower bound values.  Note that the value of the column to compare
 * 	against must be a number, or the effect of this match will be unpredictable.
 * 
 * 	@since 1.0
 * 	@author Jonathan Liang
 *  @version $Revision: 111 $ 
 */
public class RangeFilter 
	extends CDataFilter
{
	
	
	//	--- Instance Variables ---


	/** Floor value (lower-bound) */
	private double lbound = -1.0;

	/** Ceiling value (upper-bound) */
	private double ubound = -1.0;
	
	/** True, if floor / ceiling values included */
	private boolean inclusive = false;
	
	
	//	--- Constructor(s) ---

	/**
	 * Creates a range filter.  Note that all comparisons will be done via the double datatype.
	 * Its just better implemented in this manner. 
	 *
	 * @param columnName
	 * @param columnIndex
	 * @param lbound
	 * @param ubound
	 * @param inclusive
	 */
	public RangeFilter(String columnName, double lbound, double ubound, boolean inclusive)
		throws CDataGridException
	{
		super(columnName);

		if (lbound >= ubound)
			throw new CDataGridException("Lower bound cannot equal upper bound.");
		
		this.lbound = lbound;
		this.ubound = ubound;
		this.inclusive = inclusive;
		
	}
	
	//	--- Instance Methods --- 
	
	/**
	 * Performs a range-based match on given row column's value.  
	 * 
	 * @return true, if this filter matches (and false if there are no matches)
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
				if (number >= lbound && number <= ubound)
					return true;
			}	
			else 
			{
				//	Exclusive match - equality on lbound/ubound is not a match 
				if (number > lbound && number < ubound)
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
		sbuf.append("RangeFilter :: where ");
		sbuf.append(columnName).append(" (").append(columnIndex).append(") in (");
		sbuf.append(String.valueOf(lbound)).append("..").append(String.valueOf(ubound)).append("), ");
		if (inclusive)  sbuf.append("inclusive.");
		else sbuf.append("exclusive.");
		
		return sbuf.toString();
	}
	
	
	
}
