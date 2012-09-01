//	LEFilter.jav 
//	- Casper Datasets (R) -
//

package net.casper.data.model.filters;

//	Casper imports 
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRow;
import net.casper.data.model.CRowMetaData;

/**
 * This filter returns true, if the data is less than a particular upper bound. 
 * 
 * @since 1.0
 * @author Jonathan Liang
 * @version $Revision: 111 $ 
 */
public class LEFilter 
	extends CDataFilter
{
	
	//	--- Instance Variables ---

	/** Matching values will be (< ubound) */
	private double ubound = -1;

	/** True, if ubound included in match */
	private boolean inclusive = false; 
	
	
	
	//	--- Constructor(s) ---



	/**
	 * Constructs this filter 
	 * 
	 * @param columnName
	 * @param columnIndex
	 * @param ubound
	 * @param inclusive
	 * @throws CDataGridException
	 */
	public LEFilter(String columnName, double ubound, boolean inclusive)
		throws CDataGridException
	{
		super(columnName);

		this.ubound = ubound;
		this.inclusive = inclusive; 
	}
	
	
	
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
				if (number <= ubound)
					return true;
			}	
			else
			{
				//	Exclusive match - equality on lbound/ubound is not a match 
				if (number < ubound)
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
		sbuf.append("LEFilter :: where ");
		sbuf.append(columnName).append(" (").append(columnIndex).append(") in (");
		sbuf.append("<").append(String.valueOf(ubound)).append("), ");
		if (inclusive)  sbuf.append("inclusive");
		else sbuf.append("exclusive");
		
		return sbuf.toString();
	}
	
}
