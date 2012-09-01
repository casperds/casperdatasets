//	CDataComparator.java
//	- Casper Datasets (R) -
//

package net.casper.data.model; 


//	Java imports
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;



/**
 *	Data sorting functionality - implements comparator.
 *	Because we are comparing rows that can take on any data types, the constructor
 *	expects the data type of the specified column to be passed, and allows 
 *	sorting comparisons to be performed on a data type basis. 
 *  <br/><br/>
 *	Composite column sorting has been added, to allow for sorting on multiple 
 *	columns, in different orders. 
 *
 *	@since 1.0
 *	@author Jonathan Liang
 *  @version $Revision: 111 $
 */
public class CDataComparator
	implements Comparator, Serializable
{

	//	--- Static Variables ---
	
	/**
	 * 	Why do we have this ?  In the case where two objects have -NOT- even been compared yet	
	 * 	and the value of one object is null, then we cannot just return the "previous" comparison.	
	 * 	If two rows have already been compared on this basis, then we return the previous comparison.
	 */
	private static final int UNCOMPARED_STATE = -100;
	
	
	//	--- Instance Variables ---
	
	/** The indices of the column in the row(s) that we want to test for this comparison */
	private int[] columnIndices  = new int[0];

	/** 
	 *	The type of comparison.  This allow us to perform different kinds
	 *	of sorting / comparisons based on the datatype of a particular column 
	 */
	private Class[] columnTypes = new Class[0];

	

	//	--- Constructor(s) --- 

	/**
	 *	Do not allow empty instantiation.  
	 */
	private CDataComparator()
	{
	}


	/**
	 *	Construct this object 
	 *
	 *	@param columnIndex - the column that we are comparing against 
	 *	@param columnType - the datatype of the column against which we are comparing
	 *	@throws CDataGridException
	 */
	public CDataComparator(int[] columnIndices, Class[] columnTypes)
		throws CDataGridException
	{
		super();

		//	Perform error checking, cardinalities
		if (columnIndices == null || columnIndices.length < 1 || columnTypes == null ||
			columnIndices.length != columnTypes.length)
		{
			throw new CDataGridException("Size of columnIndices, ascending, and columnTypes *MUST* be equivalent.");
		}
			
		//	Check columnIndices
		for (int i = 0; i < columnIndices.length; i++) {
			if (columnIndices[i] < 0)
				throw new CDataGridException("Column index must be > 0, or a valid column name");
		}
		
		//	Check columnTypes 
		for (int i = 0; i < columnTypes.length; i++) {
			if (columnTypes[i] == null)
				throw new CDataGridException("Passed column types cannot be null.");
		}

		//	Assign all values 
		this.columnIndices = columnIndices;
		this.columnTypes = columnTypes;

	}

	

	//	--- Interface Methods ---
	
	
	/**
	 *	Compares two rows, returns an order for the comparison.
	 *	Allows for special comparisons based on datatype.  Otherwise we return
	 *	a natural ordering of the two types. 
	 *
	 *	@param o1 - first object of comparison
	 *	@param o2 - second object of comparison 
	 *	@return a negative integer if o1 < o2, zero if o1 equals o2, or a positive integer if o1 > o2  
	 */
	public int compare(Object o1, Object o2)
	{
		//	Null comparisons 
		if (o1 == null && o2 == null) { return  0; }
		if (o1 == null && o2 != null) { return -1; }
		if (o1 != null && o2 == null) { return  1; }
		
		
		//	Current comparison result. 
		int cmpResult = UNCOMPARED_STATE;

		//	In this case, both objects are NOT null, so perform comparison
		//	based upon data type of column  
		if (o1 != null && o2 != null)
		{
			//	Keep comparing, until a tie is broken or all columns have been
			//	compared.  If a tie still results, then it remains a tie. 
			for (int i = 0; i < columnIndices.length; i++)
			{		
			
				Object v1 = null;
				Object v2 = null;
				
				try
				{
					//	Ensure that both objects are of the correct type. 
					CDataRow r1 = (CDataRow) o1;
					CDataRow r2 = (CDataRow) o2;
	
					//	Values in selected column
					v1 = r1.getValue(columnIndices[i]);
					v2 = r2.getValue(columnIndices[i]);
					
					//	Return prev results, if both values null
					if (cmpResult == UNCOMPARED_STATE)
					{
						//	Rows have NOT been compared yet.  In this case, 
						//	Check nulls exhaustively, putting null values in front of non-nulls.
						if (v1 == null && v2 == null) { return  0; }
						if (v1 == null && v2 != null) { return -1; }
						if (v1 != null && v2 == null) { return  1; }					
					}
					else
					{
						//	The difference here is, if there exists a prior comparison, return
						//	prior results, if both are null.  Otherwise, nullified value is minimized 
						if (v1 == null && v2 == null) { return cmpResult; }
						if (v1 == null && v2 != null) { return -1; }
						if (v1 != null && v2 == null) { return  1; }					
					}
					
					
					//
					//	We can revisit this later. 
					//
					if (columnTypes[i].equals(String.class))
					{
						String s1 = (String) v1;
						String s2 = (String) v2;
						cmpResult = s1.compareTo(s2);
					}
					else if (columnTypes[i].equals(Boolean.class) || columnTypes[i].equals(boolean.class))
					{
						// if b1 and b2 the same, result = 0
						// if b1 is true (and b2 false), result = 1
						// if b1 is false (and b2 true), result = -1
						boolean b1 = ((Boolean)v1).booleanValue();
						boolean b2 = ((Boolean)v2).booleanValue();
						cmpResult = (b1 == b2) ? 0 : (b1 ? 1 : -1);
					}
					else if (columnTypes[i].equals(Integer.class) || columnTypes[i].equals(int.class))
					{
						Integer i1 = (Integer) v1;
						Integer i2 = (Integer) v2;
						cmpResult = i1.compareTo(i2);
					}
					else if (columnTypes[i].equals(Double.class)  || columnTypes[i].equals(double.class))
					{
						Double d1 = (Double) v1;
						Double d2 = (Double) v2;
						cmpResult = d1.compareTo(d2);
					}
					else if (columnTypes[i].equals(Float.class)  || columnTypes[i].equals(float.class))
					{
						Float f1 = (Float) v1;
						Float f2 = (Float) v2;
						cmpResult = f1.compareTo(f2);
					}
					else if (columnTypes[i].equals(Date.class))
					{
						Date dt1 = (Date) v1;
						Date dt2 = (Date) v2;
						cmpResult = dt1.compareTo(dt2);
					}
					else if (columnTypes[i].equals(Timestamp.class))
					{
						Timestamp ts1 = (Timestamp) v1;
						Timestamp ts2 = (Timestamp) v2;
						cmpResult = ts1.compareTo(ts2);
					}
					else if (columnTypes[i].equals(Byte.class) || columnTypes[i].equals(byte.class))
					{
						Byte bt1 = (Byte) v1;
						Byte bt2 = (Byte) v2;
						cmpResult = bt1.compareTo(bt2);
					}
					else
					{
						// don't know how to compare type, so return equal
						cmpResult = 0;					
					}
					
					//
					//	In this case, a tie has been broken, so break and return result
					//
					if (cmpResult != 0)
						break;
	
				}
				catch (Exception ex)
				{
					// Re-throw as runtime exception
					// with context information
					throw new RuntimeException("Failed sort comparison for columnIndex: " 
							+ columnIndices[i] + ", columnType: " + columnTypes[i] 
                            + ", value1: " + v1
                            + ", value2: " + v2
							+ ", Reason: "
							+ ex.toString());
				}
			}
		}
		
		//	Failed comparison :: both are equivalent 
		return cmpResult;
		
	}
		



	/**
	 *	Not sure why we have this, but FINE... 
	 */
	public boolean equals(Object o)
	{
		return super.equals(o);
	}
	
	
}


