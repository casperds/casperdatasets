//	CDataCacheDBAdapter.java
//	- Casper Datasets (R) -
//


package net.casper.data.model;

//	Java imports
import java.math.*;
import java.util.*;
import java.sql.*;


/**	
 * 	Database adapter class.  This static "mapper", takes a JDBC resultset
 * 	and transforms the resultset into an instance of CDataCacheContainer.
 * 	As part of this, constructs the meta-definition directly from the 
 * 	resultset metadata abstraction. 
 * 	<br/><br/>
 * 	Note: this effectively allows a user to run a sql or procedure and 
 * 	construct the dataset automatically, without additional programming. 
 *  <br/><br/>
 * 	<b>::: WARNING :::</b>
 * 	<br/><br/>
 * 	Warning: the -major- limitation of this automatic methodology is the 
 * 	primary key field, which is not exposed by the resultset.  (or doesn't 
 * 	exist for some datasets.)  If a PK is not defined, the meta-definition 
 * 	cannot be constructed.  We have to make an egregious assumption here if PK is 
 * 	not provided, and assume that the first column in the resultset is the primary 
 * 	key.   If your resultset does not fit within this criteria, please pass your
 * 	the primary keys, or your data may be overwritten.<br/>
 * 
 * @since v1.0
 * @author Jonathan Liang
 * @version $Revision: 111 $
 */
public final class CDataCacheDBAdapter 
{

	//	--- Constructor(s) ---
	
	/**
	 * Disallow private instantiation 
	 */
	private CDataCacheDBAdapter()
	{
	}

	
	//	--- Static Methods --- 
	
	/**
	 * 	Loads data from database, given particular columns that the user wishes to load. 
	 * 
	 * 	@param rs
	 * 	@param cacheName
	 * 	@param columnNames
	 * 	@param primaryKeys
	 * 	@param concreteMap
	 * 	@return CDataCacheContainer
	 * 	@throws CDataGridException 
	 */
	public static CDataCacheContainer loadData(ResultSet rs, String cacheName, 
											   String[] columnNames, String[] primaryKeys, Map concreteMap)
		throws CDataGridException
	{
		
		if (rs == null)
			throw new CDataGridException("Resultset to load / transform cannot be null.");
		if (columnNames == null || columnNames.length < 1 || primaryKeys == null || primaryKeys.length < 1)
			throw new CDataGridException("Column names and primary key(s) cannot be null.");
		
		
		CRowMetaData metaDefinition = null;
		
		//	List of types for the requested column names 
		Class[] classTypes = new Class[0];
		
		
		try
		{
			//	Meta definition. 
			ResultSetMetaData meta = rs.getMetaData();
			classTypes = new Class[columnNames.length];

			int[] rawColumnTypes = new int[columnNames.length];
			Class[] trsColumnTypes = new Class[columnNames.length];

			for (int i = 0; i < columnNames.length; i++) 
			{
				int rsColIndex = rs.findColumn(columnNames[i]);
				rawColumnTypes[i] = meta.getColumnType(rsColIndex);
				trsColumnTypes[i] = getTransfClassType(rawColumnTypes[i]);
			}
			
						
			metaDefinition = new CRowMetaData(columnNames, trsColumnTypes, primaryKeys);
		
		}
		catch (Exception ex)
		{
			throw new CDataGridException(ex.toString(), ex);
		}
	
		//	Load data, given provided column types
		return loadData(rs, cacheName, metaDefinition, concreteMap);

	}
	
	
	
	/**
	 * 	Given a JDBC resultset, read all data and meta data definition
	 * 	and save into an in-memory CDataCacheContainer.  
	 * 	First reads a meta-definition, then sends to data loader
	 * 
	 * @param rs - the resultset to transform into a CDataCacheContainer
	 * @return CDataCacheContainer object holding data from this resultset 
	 * @throws CDataGridException
	 */
	public static CDataCacheContainer loadData(ResultSet rs, String cacheName, String[] primaryKeys, Map concreteMap)
		throws CDataGridException
	{
		//	Check null errors 
		if (rs == null)
			throw new CDataGridException("Resultset to load / transform cannot be null. ");

		//	Meta-data definition for this cache
		CRowMetaData metaDefinition = null;
		
		try
		{

			//	-- LOAD META-DEFINITION --
						
			//	Meta definition. 
			ResultSetMetaData meta = rs.getMetaData();
			int numColumns = meta.getColumnCount();
	
			
			//	Derive column names, raw SQL types, java class types
			String[] columnNames = new String[numColumns];
			int[] rawColumnTypes = new int[numColumns];
			Class[] trsColumnTypes = new Class[numColumns];

			for (int i = 0; i < columnNames.length; i++) {
				columnNames[i] = meta.getColumnName((i + 1));
				rawColumnTypes[i] = meta.getColumnType((i + 1));
				trsColumnTypes[i] = getTransfClassType(rawColumnTypes[i]);
			}
			
			metaDefinition = new CRowMetaData(columnNames, trsColumnTypes, primaryKeys);

		}
		catch (Exception ex)
		{
			//	Re-throw as a local exception 
			throw new CDataGridException(ex.toString(), ex);
		}
	
		//	
		return loadData(rs, cacheName, metaDefinition, concreteMap);

	}
	

	/**
	 * Given a JDBC resultset, and a predefined meta-definition object, create a cache container 
	 * 
	 * @param rs
	 * @param cacheName
	 * @param metaDefinition
	 * @param concreteMap
	 * @return
	 * @throws CDataGridException
	 */
	public static CDataCacheContainer loadData(ResultSet rs, String cacheName, CRowMetaData metaDefinition, Map concreteMap)
		throws CDataGridException
	{
		//	Check null errors 
		if (rs == null)
			throw new CDataGridException("Resultset to load / transform cannot be null. ");

		if (metaDefinition == null)
			throw new CDataGridException("Meta definition cannot be null.");
		
		//	Cache container 
		CDataCacheContainer container = null;
		
		try
		{

			//	Column names / types 
			String[] columnNames = metaDefinition.getColumnNames();
			Class[]  columnTypes = metaDefinition.getColumnTypes();
			int numColumns = columnNames.length;
			

			//
			//	--- LOAD DB-RESULTSET DATA ---
			//
			
			//	List of all data objects 
			LinkedList list = new LinkedList(); 

			//	Iterate through entire set, appending to list. 
			while (rs.next())
			{
				//	Create new data row. 
				CDataRow row = new CDataRow(numColumns);
				
				//	Iterate through all existing columns 
				for (int cDataColIdx = 0; cDataColIdx < columnNames.length; cDataColIdx++)
				{
					String columnName = columnNames[cDataColIdx];
					Class columnClass = columnTypes[cDataColIdx];
					Object columnData = null;
					
					try
					{
						//	Try to find the column in the resultset.  If the column doesn't exist, 
						//	do not import the object into the memory rowset 
						int jdbcColIdx = -1;
						if ((jdbcColIdx = rs.findColumn(columnName)) >= 0)
						{
							// Get column data, TYPE -SAFE- 
							if (columnClass == Boolean.class) 		  { columnData = CDataConverter.getBoolean(rs, jdbcColIdx); }
							else if (columnClass == Byte.class) 	  { columnData = CDataConverter.getByte(rs, jdbcColIdx); }
							else if (columnClass == Short.class) 	  { columnData = CDataConverter.getShort(rs, jdbcColIdx); }
							else if (columnClass == Integer.class) 	  { columnData = CDataConverter.getInt(rs, jdbcColIdx); }
							else if (columnClass == Float.class) 	  { columnData = CDataConverter.getFloat(rs, jdbcColIdx); }
							else if (columnClass == Long.class) 	  { columnData = CDataConverter.getLong(rs, jdbcColIdx); }
							else if (columnClass == Double.class) 	  { columnData = CDataConverter.getDouble(rs, jdbcColIdx); }
							else if (columnClass == String.class) 	  { columnData = CDataConverter.getString(rs, jdbcColIdx); }
							else if (columnClass == Timestamp.class)  { columnData = CDataConverter.getDate(rs, jdbcColIdx); }
							else if (columnClass == java.sql.Date.class) { columnData = CDataConverter.getDate(rs, jdbcColIdx); }
							else if (columnClass == java.util.Date.class) { columnData = CDataConverter.getDate(rs, jdbcColIdx); }
							else if (columnClass == null) 			  { columnData = null; }
							else { throw new CDataGridException("Unable to handle column types of class: " + columnClass.getName()); }
						
							//	Set particular value within row 
							row.setValue(cDataColIdx, columnData);
						}
					}
					catch (SQLException sqlex)
					{
					}
				}
				
				list.add(row);
			}
			
			//	Convert list to array of CDataRow objects
			CDataRow[] rows = new CDataRow[list.size()];
			list.toArray(rows);

			
			//	-- CONSTRUCT CONTAINER --
			container = new CDataCacheContainer(cacheName, metaDefinition, concreteMap);
			container.addData(rows);
			
		}
		catch (Exception ex)
		{
			//	Re-throw as a local exception 
			throw new CDataGridException(ex.toString(), ex);
		}
		
		
		//	Return resulting container 
		return container; 

	}
	
	
	
	
	/**
	 *  Given a particular type of class, return a "transformed" class.  
	 *  This is necessary if there are particular SQL types that the dataset implementation
	 *  does not support.  (i.e. binaries or blobs, etc), which will also allow the implementation
	 *  to throw an exception.  Once again, if a user really needs to load a particular type, then
	 *  (s)he can write a custom loader. 
	 * 
	 * 	@param inClass
	 * 	@return transformed class type 
	 * 	@throws CDataGridException
	 */
	public static Class getTransfClassType(int columnType) 
		throws CDataGridException
	{
		// Get column class.
		Class columnClass = null;

		switch (columnType)
		{
			case Types.BIT:
				columnClass = Boolean.class;
				break;
			case Types.TINYINT:
				columnClass = Integer.class;
				break;
			case Types.SMALLINT:
				columnClass = Short.class;
				break;
			case Types.INTEGER:
				columnClass = Integer.class;
				break;
			case Types.REAL:
				columnClass = Float.class;
				break;
			case Types.FLOAT:
			case Types.DOUBLE:
				columnClass = Double.class;
				break;
			case Types.CHAR:
			case Types.LONGVARCHAR:
			case Types.VARCHAR:
				columnClass = String.class;
				break;
			case Types.DATE:
			case Types.TIME:
			case Types.TIMESTAMP:
				columnClass = java.util.Date.class;
				break;
			case Types.NULL:
				columnClass = null;
				break;
			default:
				columnClass = String.class;   // Make this string, if cannot be found
		}
		
		return columnClass;		
	
	}
	

}
