//	EqualsFilter.java
//	- Casper Datasets (R) -
//

package net.casper.data.model.filters;



//	Casper imports
import net.casper.data.model.*;


/**
 * 	This is the most basic type of filter.  Performs an equality-based match
 * 	on a given list of values.  If any of the values in the list "matchValues" matches
 * 	the value of the column provided, then "true" will be returned. 
 * 	
 * 	@since 1.0
 * 	@author Jonathan Liang
 *  @version $Revision: 111 $ 
 */
public class EqualsFilter 
	extends CDataFilter
{

	//	--- Instance Variables --- 
	
	/** Values to match the equals filter upon */
	private Object[] matchValues = new Object[0];
	
	/** Negated setting (in this case, will return the opposite of the current match) */
	private boolean negated = false;

	
	//	--- Constructor(s) ---

	/**
	 *	Do not allow empty instantiation 
	 */
	private EqualsFilter()
		throws CDataGridException
	{
		super(null);
	}
	

	/**	
	 * 	Performs equality-based filtering on a given list of values. 
	 * 
	 * 	@param columnName - column name to match upon 
	 * 	@param matchValues - values on which to perform matching. 
	 * 	@throws CDataGridException 
	 */
	public EqualsFilter(String columnName, Object[] matchValues)
		throws CDataGridException
	{
		//	Call parent
		super(columnName);
		
		if (matchValues == null || matchValues.length < 1)
			throw new CDataGridException("Values to match this column cannot be null.");
	
		this.matchValues = matchValues; 
	}
	

	/**
	 * Performs equality based filtering, with the option to return the negated 
	 *  
	 * @param columnName
	 * @param metaDef
	 * @param matchValues
	 * @param negated
	 * @throws CDataGridException
	 */
	public EqualsFilter(String columnName, Object[] matchValues, boolean negated)
		throws CDataGridException
	{
		this(columnName, matchValues);
		this.negated = negated;
	}
	
	
		
	//	--- Interface Methods --- 
	
	/**
	 * Performs a simple, equality-based match on this filter. 
	 * 
	 * @return true, if this filter matches (and false if there are no matches)
	 * @throws CDataGridException
	 */
	public boolean doesMatch(CDataRow row)
		throws CDataGridException
	{
		//	Check col index initialization 
		checkColumnIndexInitialized();
		
		boolean match = false;
		
		if (matchValues != null && matchValues.length > 0)
		{
			for (int i = 0; i < matchValues.length; i++) 
			{
				if (matchValues[i] == null)
				{
					//	Null match 
					if (row.getValue(columnIndex) == null) {
						match = true;
						break;
					}
				}
				else
				{
					//	Non-null equality match 
					if (matchValues[i].equals(row.getValue(columnIndex))) {
						match = true;
						break;
					}
				}
			}
		}
		
		//	If negated, then reverse the decision
		if (negated)
			return (!match);
		
		return match;
	}

	

	/**
	 *	Returns all match values
	 *	@return Object[] of match values 
	 */
	public Object[] getMatchValues() {
		return matchValues;
	}
	
	
	/**
	 * 	Returns string representation of this object
	 * 	@return string
	 */
	public String toString()
	{
		StringBuffer sbuf = new StringBuffer();
		sbuf.append("EqualsFilter :: where ");
		sbuf.append(columnName).append(" (").append(columnIndex).append(") in: ");

		sbuf.append("(");
		if (matchValues != null)
		{
			for (int i = 0; i < matchValues.length; i++) 
			{
				sbuf.append(matchValues[i]);
				if (i < matchValues.length - 1)
					sbuf.append(", ");
			}
		}
		sbuf.append(")");
		
		return sbuf.toString();
	}
	
	
}
