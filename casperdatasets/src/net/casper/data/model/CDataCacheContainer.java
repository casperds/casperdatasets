//	CDataCacheContainer.java
//	- Casper Datasets (R) -
//

package net.casper.data.model;


//	Java imports
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.casper.data.model.filters.CDataFilterClause;
import net.casper.data.model.filters.EqualsFilter;


/**
 *	The cache container serves as the implementation container class for 2-D data caches.
 *	Provides structural, behavioral, and filtering functionalities on a variable-sized dataset cache.
 *	Note, this is a thread-safe container, which allows for concurrent modification.
 *	However, it does sacrifice accuracy on reads if there are high rates of additions and removals from
 *	the cache.  This implementation decision was made on the basis of a high-volume read, low-volume write 
 *	assumption.  
 *	<br/><br/>
 *	Rows are indexed by primary key.  
 *	Primary keys should be designated with caution, as duplicates will cause
 *	silent overwrites of existing rows in the cache container.  The data row map should be
 *	initializable with any type of concrete Map implementation (this will accommodate 
 *	distributed caching services or products)
 *	<br/><br/>
 *	Major Components
 *	<br/><br/>
 *	1. Structural information (meta-data): Column names, Types, Primary key configurations <br/>
 *	2. Structural modifiers: indices on data rows (for performance optimization) <br/>
 *	3. Data store: stores references to rows of data, keyed by each data row's primary key <br/>
 *	4. Data manipulation: add / remove data from grid. <br/>
 *	5. Data actions: filtering (searching).  Different types of filters can be accepted in the future. <br/>
 *	<br/>
 *	@since v1.0
 *	@author Jonathan Liang
 *  @version $Revision: 125 $ 
 */
public class CDataCacheContainer
	implements Serializable
{

	//
	//	--- Static Variables ---
	//
	
	/**	Required for serializable */
	private static final long serialVersionUID = 1L;
	
	
	//
	//	--- Instance Variables ---
	//

	/** Data storage.  The concrete implementation is passed into the container's constructor. */
	private Map dataRowMap = null;
	
	/** Name of the cache */
	private String cacheName = null;
	
	/** Meta-data object (describing column names, datatypes, etc) */
	private CRowMetaData metaData = null;	
		
	/** Map of indices (by column) created for this cache */
	private Map indexMap = new HashMap();

	/** This locks the entire cache while it is being updated. */
	private transient Object lock = new Object();

	/** Identity primary key. Used by containers that have a null primary key */
	private int identityPK = 0;

	//
	//	--- Constructor(s) ---
	//

	/**
	 *	Empty instantiation not allowed. 
	 */
	private CDataCacheContainer()
	{
	}

	/**
	 * Construct a new CDataCacheContainer from a builder.
	 * 
	 * @param builder builder
	 * @return a CDataCacheContainer built from {@code builder}.
	 * @throws CDataGridException if problem creating container. 
	 */
	public CDataCacheContainer(CBuilder builder) 
		throws CDataGridException {

		try {
			
			builder.open();

			init(builder.getName(), new CRowMetaData(
					builder.getColumnNames(), builder.getColumnTypes(),
					builder.getPrimaryKeyColumns()), builder.getConcreteMap());

			Object[] nextRow = null;
				
			List cRows = new LinkedList();
			
			while ((nextRow = builder.readRow()) != null) {
				// Create a new CDataRow from the read in row
				// and add it to the container
				CDataRow cRow = new CDataRow(nextRow);
				cRows.add(cRow);
			}
			
			// Convert list to array of CDataRow objects
			CDataRow[] cRowsArray = new CDataRow[cRows.size()];
			cRows.toArray(cRowsArray);
			
			// Add all rows at once (more efficient than 
			// one by one) and update indices.
			addData(cRowsArray);

			builder.close();
			
		} catch (IOException e) {
			builder.close();
			throw new CDataGridException("Error building container", e);
		}
	}

	
	/**
	 * Creates an instance of a cache container.
	 * In this case, the underlying map storage implementation will be java.util.HashMap.
	 * 
	 * 	@param cacheName - name of cache
	 * 	@param metaData - the meta definition
	 * 	@throws CDataGridException
	 */
	public CDataCacheContainer(String cacheName, CRowMetaData metaData)
		throws CDataGridException
	{
		//	Call overloaded, w/ java.util.HashMap instance 
		this(cacheName, metaData, new HashMap());
	}


	/**
	 *	Creates an instance of this cache container 
	 *	Note that, ultimately, the underlying map implementation is 
	 *	passed in.  This will allow the dynamic swap-in of 
	 *	cache implementations (or existing cached objects)
	 *
	 *	@param cacheName - name of cache
	 *	@param metaData - meta data object describing the cache's configuration 
	 *	@param dataRowMap - the concrete implementation 
	 *	@throws CDataGridException
	 */
	public CDataCacheContainer(String cacheName, CRowMetaData metaData, Map dataRowMap)
		throws CDataGridException
	{
		init(cacheName, metaData, dataRowMap);
	}

	/**
	 * Convenience method for construction of a {@link CDataCacheContainer}.
	 * Uses a {@link HashMap} for data row storage.
	 * 
	 * @param cacheName
	 *            of cache
	 * @param columnNames
	 *            column names. Multiple columns are separated by commas, eg:
	 *            "firstname,lastname" (NB: do not include a space after the
	 *            comma).
	 * @param columnTypes
	 *            column types
	 * @param primaryKeys
	 *            primary key. Multiple columns are separated by commas, eg:
	 *            "firstname,lastname" (NB: do not include a space after the
	 *            comma).
	 * @throws CDataGridException
	 *             if problem during construction.
	 */
	public CDataCacheContainer(String cacheName, String columnNames,
			Class[] columnTypes, String primaryKeys)
			throws CDataGridException {

		if (columnNames == null) {
			throw new CDataGridException(
					"Cannot create container: column names are null");
		}

		CRowMetaData metaDef =
			new CRowMetaData(columnNames.split(","), columnTypes,
					(primaryKeys == null) ? null : primaryKeys.split(","));
		init(cacheName, metaDef, new HashMap());

	}
	
	/** 
	 * Main constructor work is done here. This allows the builder constructor to
	 * call this after opening the builder.
	 *  
	 * @param cacheName - name of cache
	 * @param metaData - meta data object describing the cache's configuration 
	 * @param dataRowMap - the concrete implementation 
	 * @throws CDataGridException
	 */
	private void init(String cacheName, CRowMetaData metaData, Map dataRowMap)
		throws CDataGridException {
		
		//	Error checking 
		if (metaData == null)
			throw new CDataGridException("Meta data object cannot be null.");
		if (dataRowMap == null)
			throw new CDataGridException("Data map object cannot be null.");
		
		//	Cache name 
		this.cacheName = cacheName;
		
		//	Meta-data object 
		this.metaData = metaData;

		//	Concrete map implementation
		this.dataRowMap = dataRowMap;

	}
	
	/**
	 * Creates a new {@link CDataCacheContainer} with rows in order of insertion.
	 * 
	 * @throws CDataGridException if problem creating container 
	 */
	public static CDataCacheContainer newInsertionOrdered(String cacheName,
			String columnNames, Class[] columnTypes) throws CDataGridException {
		

		// create a container without a primary key
		// this will mean the primary key is the identity PK
		CRowMetaData metaDef =
			new CRowMetaData(columnNames.split(","), columnTypes,
					null);
		
		// use a LinkedHashMap so that the primary key (ie: the identity) is ordered
		return
				new CDataCacheContainer(cacheName, metaDef, new LinkedHashMap());

	}

	
	//	
	//	--- Instance Methods --- 
	//
	
	/**
	 * 	Returns size of cache
	 * 	@return cardinality of cache container 
	 */
	public int size() 
	{
		if (dataRowMap == null)
			return 0;

		return dataRowMap.size();
	}
	
	/**
	 * Returns size of cache 
	 * @return cardinality of cache container 
	 */
	public int getNumberRows() {
		return size(); 
	}

	/**
	 * Returns name of cache 
	 * @return name of cache
	 */
	public String getCacheName() {
		return cacheName;
	}
	
	/**
	 * Return meta definition for this cache
	 * @return
	 */
	public CRowMetaData getMetaDefinition() {
		return this.metaData;
	}
	
	
	/**
	 * Export this CDataCacheContainer via an exporter.
	 * 
	 * @param exporter exporter
	 * @throws CDataGridException if problem exporting container
	 */
	public Object export(
			CExporter exporter) throws CDataGridException  {
		
		try {
			CRowMetaData meta = getMetaDefinition();
			
			exporter.setName(getCacheName());
			exporter.setColumnNames(meta.getColumnNames());
			exporter.setColumnTypes(meta.getColumnTypes());
			exporter.setPrimaryKeyColumns(meta.getPrimaryKeyColumns());
			exporter.open();
			
			CDataRowSet rowset = getAll();
			
			while (rowset.next()) {
				CDataRow cRow = rowset.getCurrentRow();
				exporter.writeRow(cRow.getRawData());
			}
	
		} catch (IOException e) {
			exporter.close();
			throw new CDataGridException("Error exporting container: " + e.getMessage(), e);
		}

		return exporter.close();
	
	}

	
	/**
	 * Returns matching data, sorted by PK (by default)
	 * 
	 * @param columnName
	 * @param values
	 * @return
	 * @throws CDataGridException
	 */
	public CDataRowSet get(String columnName, Object[] values)
		throws CDataGridException
	{
		return get(columnName, values, null, true);
	}

	
	/**
	 *	Returns a collection of data objects.  
	 *	This overloaded method makes it easier for a user to 
	 *	perform a multi-value match using equality-based matching.  This method merely constructs
	 *	an equality-based filter into a filter clause to perform the general search on records in this cache. 
	 *
	 *	@param columnName - column to perform match on 
	 *	@param values - values to match 
	 *	@param sortColumnNames - names of columns to sort data by 
	 *	@param ascending - true, to return in ascending order; false to reverse
	 *	@return CDataRowSet - rowset of all matching rows 
	 *	@throws CDataGridException 
	 */
	public CDataRowSet get(String columnName, Object[] values, String[] sortColumnNames, boolean ascending)
		throws CDataGridException
	{
		//	Construct a filter clause, based on equality values; 
		EqualsFilter filter = new EqualsFilter(columnName, values);
		CDataFilterClause filterClause = new CDataFilterClause();
		filterClause.addFilter(filter);
		
		//	Return matching rowset
		return get(filterClause, sortColumnNames, ascending);
	}


	/**
	 * Returns results from cache, sorted by PK (by default), ascending. 
	 * 
	 * @param filterClause
	 * @return rowset of matching rows
	 * @throws CDataGridException
	 */
	public CDataRowSet get(CDataFilterClause filterClause)
		throws CDataGridException
	{
		//	Call overloaded method, w/ default sorting on first PK column
		return get(filterClause, null, true);
	}
	
	
	/**
	 * Returns all rows from the cache which match the filters provided. 
	 * Checks if there exists an index on the column.  Match according to passed multi-values. 
	 * Then sorts data matches. 
	 * 
	 * @param columnName
	 * @param filters
	 * @param sortColumnName
	 * @param ascending
	 * @return collection of all matching rows in the cache 
	 * @throws CDataGridException
	 */
	public CDataRowSet get(CDataFilterClause filterClause, String[] sortColumnNames, boolean ascending)
		throws CDataGridException 
	{
		//	No filters configured.  Return full dataset.
		if (filterClause.size() < 1)
			return getAll(sortColumnNames, ascending);

		//	Perform match search
		//	long startTime = System.currentTimeMillis();
		
		//	Matching rows 
		CDataRow[] rows = new CDataRow[dataRowMap.size()];
		dataRowMap.values().toArray(rows);

		//	Set meta definition - this will be used during the query 
		//	by all filters to find the column indices given the column names
		filterClause.setMetaDefinition(metaData);

		//	Optimizations 
		//	Primary Keys and Indices 
		//	Set the container as a call back.  This call back will be used in the event
		//	that there exists a search on the primary key or index on this. 
		filterClause.setCacheContainerCallbackOptimization(this);

		//	Search, w/ PK & Index Optimizations 
		//	Perform search on all filters.  The filter clause will manage optimizations. 
		//	Will perform PK and index searches.  And will perform a full table scan in 
		//	the worst case. 
		rows = filterClause.match(rows);
		
		//	Assemble final rowset object, sort, return; 
		CDataRowSet rowset = new CDataRowSet(metaData);
		rowset.addData(rows);
		if (sortColumnNames != null && sortColumnNames.length > 0)
			rowset.sortByColumn(sortColumnNames, ascending);

		// 	long endTime = System.currentTimeMillis();
		// 	long elapsed = endTime - startTime;		
		//	Log some search information
		// 	System.out.println("CDataCacheContainer :: Retrieved " + rowset.getNumberRows() + " rows.  Filter / sort performance: " + elapsed + " ms.  Filter Query: " + filterClause.toString());

		
		return rowset;
	}
	

	/**
	 *	Returns all rows, sorted by primary key.
	 *
	 *	@throws CDataGridException
	 *	@return CDataRowSet - rowset of matching rows 
	 */
	public CDataRowSet getAll()
		throws CDataGridException
	{
		//	By default, return by order of one primary key field, ascending order.
		return getAll(metaData.getPrimaryKeyColumns(), true);
	}


	/**
	 *	Returns all rows in the cache. 
	 *	Data can be sorted in a specified order on a given column 
	 *
	 *	@param sortColumnName - column to sort results by, or null if sorting not important
	 *	@param ascending - true, if sort order should be ascending
	 *	@return CDataRowSet - rowset object containing all matching rows. 
	 *	@throws CDataGridException
	 */
	public CDataRowSet getAll(String[] sortColumnNames, boolean ascending)
		throws CDataGridException
	{
		//	Convert to array format 
		Collection values = dataRowMap.values();
		CDataRow[] rows = new CDataRow[values.size()];
		values.toArray(rows);

		//	Create rowset, sort, return 
		CDataRowSet rowset = new CDataRowSet(metaData);
		rowset.addData(rows);
		
		if (sortColumnNames != null && sortColumnNames.length > 0)
			rowset.sortByColumn(sortColumnNames, ascending);

		return rowset; 
	}
	

	/**
	 * Returns all data rows in unsorted raw format. 
	 * @return
	 */
	public CDataRow[] getAllRows()
	{
		Collection values = dataRowMap.values();
		CDataRow[] rows = new CDataRow[values.size()];
		values.toArray(rows);
		return rows; 
	}
	
	
	
	/**
	 * 	Adds all rows from data container into current data container
	 * 	(via union operation).
	 * 
	 * @param dataContainer
	 * @throws CDataGridException
	 * @return number rows added
	 */
	public int addData(CDataCacheContainer dataContainer)
		throws CDataGridException
	{
		return addData(dataContainer, true);
	}
	
	
	/**
	 *	Adds rows (union operation) from a data container into the current data container. 
	 *	Note, the meta definitions will be validated before the containers can be unioned
	 *	in order to avoid corruption of data in the container. 
	 * 
	 * @param dataContainer
	 * @param updateIndices
	 * @throws CDataGridException
	 * @return number of rows added
	 */
	public int addData(CDataCacheContainer dataContainer, boolean updateIndices)
		throws CDataGridException
	{
		//	Meta-definitions not equivalent ?? 
		if (dataContainer.getMetaDefinition() == null ||
			!metaData.equals(dataContainer.getMetaDefinition()))
		{
			throw new CDataGridException("Meta definition of data containers to be merged are not equivalent.");
		}
		
		//	Add all rows from source container to current container. 
		CDataRow[] rows = dataContainer.getAllRows();
		return addData(rows, updateIndices);
	}
	
	
	
	/**
	 *	Adds a whole set of data to the data cache (union operation). 
	 *	Because we add data as well as rebuild the indices (by default)
	 *	It will be considered inefficient to call this method repeatedly. 
	 *
	 *	@param dataRows - rows to add to container
	 *	@throws CDataGridException
	 *	@return number of rows added
	 */
	public int addData(CDataRow[] dataRows)
		throws CDataGridException 
	{
		return addData(dataRows, true);
	} 
	
	/**
	 * Convenience method to add a single row of data. After calling this, particularly 
	 * if called repeatedly, call {@link #updateIndices()}.
	 *  
	 * @param row raw data row
	 * @throws CDataGridException if problem adding the row 
	 */
	public void addSingleRow(Object[] oRow) throws CDataGridException {
		CDataRow cRow = new CDataRow();
		cRow.setRawData(oRow);
		addData(new CDataRow[] { cRow });
	}
	 
	/**
	 *	Add all data rows to the grid (union operation).  The option of updating 
	 *	the indices is provided as an option here as an outlet to save on performance, 
	 *  (since the indices have to be rebuilt).
	 *
	 *	@param dataRows - rows of data to add to cache.  
	 *	@param updateIndices - true, to automatically rebuild any configured indices
	 *	@throws CDataGridException
	 */ 
	public int addData(CDataRow[] dataRows, boolean updateIndices)
		throws CDataGridException
	{
		if (dataRows == null || dataRows.length < 1)
			return 0;

		//	
		//	Data Grid Cardinality Check :: 	
		//	Check that all data rows have the proper cardinality
		//	Should also check types, but that would be quite exhaustive. 
		// 
	
		for (int i = 0; i < dataRows.length; i++) {
			if (dataRows[i] == null || dataRows[i].getNumberColumns() != metaData.getNumberColumns())
				throw new CDataGridException("Data row is corrupt: column(s) do not correspond to meta definition.");
		}
		
		int additionCount = 0;
		checkLock();
		synchronized (lock)
		{
			//	Add all data rows to cache 
			for (int j = 0; j < dataRows.length; j++) {
				
				Object primaryKey;
				if (metaData.getPrimaryKeyColumns() == null) {
					identityPK++;
					primaryKey = new Integer(identityPK);
				} else {
					// create primary key
					primaryKey = metaData.createPrimaryKey(dataRows[j]);
				}
				
				// add row to map, keyed to primary key
				// NB: if the primary key is not unique, then previously
				// written data rows will be overwritten
				dataRowMap.put(primaryKey, dataRows[j]);	
				additionCount++;
			}
				
			if (updateIndices) {
				// 	Re-build all indices 			
				updateIndices();
			}
		}	
		
		return additionCount;
	}

	
	/**
	 *	Removes all data rows matching the values provided
	 *	There are two cases for removal: 
	 *	(1)	column index is the same as that of the primary key column :: simple removal 
	 *		(this can only be the case if there is a single PK column (no composite keys))
	 *	(2) brute-force scan :: no alternative here, so we need to scan once through the dataset
	 *
	 *	@param columnName - column name to perform removal matches on 
	 *	@param values - matching values to remove from container
	 *	@param updateIndices - true, to automatically update any configured indices
	 *	@throws CDataGridException 
	 */	
	public int removeData(String columnName, Object[] values, boolean updateIndices)
		throws CDataGridException
	{
		if (values == null || values.length < 1)
			return 0;

		//	Construct a filter clause, based on equality values; 
		EqualsFilter filter = new EqualsFilter(columnName, values);
		CDataFilterClause filterClause = new CDataFilterClause();
		filterClause.addFilter(filter);

		int removalCount = removeData(filterClause, updateIndices);
		return removalCount;
	}	
		
	
	/**
	 *	Removes all records which match the filter clause provided. 
	 *
	 * @param filterClause
	 * @param updateIndices
	 * @throws CDataGridException
	 * @return number of row removed
	 */
	public int removeData(CDataFilterClause filterClause, boolean updateIndices)
		throws CDataGridException
	{	
		//	No filters, remove nothing. 
		if (filterClause == null || filterClause.size() < 1)
			return 0;

		//	Number of rows to remove 
		int removalCount = 0;

		//	Retrieve all rows matching the filter, and remove from data cache. 
		CDataRowSet removalCandidates = get(filterClause, null, true);
		CDataRow[] rows = removalCandidates.getAllRows();

		if (rows != null && 
			rows.length > 0)
		{
			checkLock();
			synchronized (lock)
			{	
				//	Re-create primary keys for matching rows, remove from underlying cache store... 
				for (int i = 0; i < rows.length; i++) {
					Object primaryKey = metaData.createPrimaryKey(rows[i]);
					dataRowMap.remove(primaryKey);
				}
				
				//	Update configured indices, if underlying cache was modified. 
				removalCount = rows.length;
				if (updateIndices && removalCount > 0)
					updateIndices();
			}
		}
	
		// System.out.println("CDataCacheContainer :: Removed " + removalCount + " rows from table via match.");		
		return removalCount;
	}
	

	/**
	 *	"Remove-all" effectively wipes out the data underlying the cache. 
	 *	We can't just replace the dataMap with a new map, because the persistent
	 *	store will still maintain reference to the data.  We need to retreive the
	 *	keyset and remove every entry, one-by-one  
	 *
	 * @throws CDataGridException
	 */
	public int removeAll()
		throws CDataGridException
	{
		int removalCount = 0; 
		if (dataRowMap != null && dataRowMap.size() > 0)
		{	
			checkLock();
			synchronized (lock)
			{
				Set keyset = dataRowMap.keySet();
				Object[] keylist = new Object[keyset.size()];
				keyset.toArray(keylist);
				for (int i = 0; i < keylist.length; i++) {
					dataRowMap.remove(keylist[i]);
					removalCount++;
				}
				
				updateIndices();
			}
			
		}	
		
		return removalCount;
	}
	
	
	/**
	 * 	Merges another data container into the current data container.  
	 * 	There are the following limitations to the merge operation: 
	 * 
	 * 	(1) Both data containers must have the same primary key
	 * 	(2) Only the data columns that exist in both the destination container (current) and
	 * 		the source container ("mergeFrom") will exist in the 
	 * 
	 * 	@param mergeFrom - CDataCacheContainer to merge into current container
	 * 	@param joinColumns - columns to join by
	 * 	@return number rows merged 
	 * 	@throws CDataGridException
	 */
	public int merge(CDataCacheContainer mergeFrom, String[] joinColumns)
		throws CDataGridException
	{
		if (mergeFrom == null)
			throw new CDataGridException("Data container to merge cannot be null.");
		
		CDataRowSet fromRowSet = mergeFrom.getAll();
		return merge(fromRowSet, joinColumns);
	}
	

	/**
	 * 	Merges a rowset into the current data container 
	 * 
	 * @param mergeFrom - the rowset to merge from
	 * @param mergeColumn - the column to join on 
	 * @return number of rows updated
	 * @throws CDataGridException
	 */
	public int merge(CDataRowSet mergeFrom, String[] joinColumns)
		throws CDataGridException
	{
		//	Perform error-checking 
		if (mergeFrom == null)
			throw new CDataGridException("Data rowset to merge cannot be null.");
		if (joinColumns == null || joinColumns.length < 1)
			throw new CDataGridException("Must join on at least one column.");
		
		//	Do not perform unnecessary work.
		if (mergeFrom.getNumberRows() < 1)
			return 0;
		
		//	Number of rows merged
		int rowsUpdated = 0;

		//	Meta information 
		CRowMetaData fromMetaDef = mergeFrom.getMetaDefinition();
		String[] fromColumnNames = fromMetaDef.getColumnNames();
		String[] destPrimaryKeys = metaData.getPrimaryKeyColumns();

		//
		//	Set of column names in destination which should -NOT- be overwritten 
		//	- Primary key fields in destination should not be overwritten
		//	- Join columns also should not be overwritten in the destination container (redundant ops)
		//
		
		HashSet nonOverwrites = new HashSet();
		for (int i = 0; i < destPrimaryKeys.length; i++) { nonOverwrites.add(destPrimaryKeys[i]); }
		for (int i = 0; i < joinColumns.length; i++)     { nonOverwrites.add(joinColumns[i]); }

		checkLock();
		synchronized (lock)
		{			
			//	Iterate through all source rows, merge into destination container, based on join columns.
			//	This will overwrite existing data in the destination container. 
			mergeFrom.reset();
			while (mergeFrom.next())
			{
				
				//	Get all *matching* rows in destination container. 
				//	Construct a query on destination container, based on equality values; 
				CDataFilterClause filterClause = new CDataFilterClause();
				for (int i = 0; i < joinColumns.length; i++)  
				{
					Object eqValue = mergeFrom.getObject(joinColumns[i]);
					EqualsFilter eqFilter = new EqualsFilter(joinColumns[i], new Object[] {eqValue});
					filterClause.addFilter(eqFilter);
				}
				
				//	Get all matching rows. 
				CDataRowSet results = get(filterClause, null, false);
				
				//	If there exist any matches, merge all available rows in from 
				//	the source rowset into the destination data container 
				if (results != null && results.getNumberRows() > 0)
				{
					//	Iterate through matching rows, setting values for all 
					//	columns in common between two rows, -except- for primary keys. 
					while (results.next())
					{
						for (int i = 0; i < fromColumnNames.length; i++)
						{
							//
							//	-- Primary Key Check -- 
							//	Check if fromColumnNames is same as primary key column.  Skip 
							//	if it is in the primary key for the destination container 
							//

							if (nonOverwrites.contains(fromColumnNames[i]))
								continue;

							//
							//	Set all available values, -ONLY- if both the 
							//	column name and column type are equivalent
							//
							
							if (metaData.containsColumn(fromColumnNames[i]) &&
								metaData.getColumnType(fromColumnNames[i]).equals(fromMetaDef.getColumnType(fromColumnNames[i])))
							{
								Object fromObjVal = mergeFrom.getObject(fromColumnNames[i]);
								results.setValue(fromColumnNames[i], fromObjVal);
							}
						}
					}
				}
				
				//	Count, number of rows updated 
				rowsUpdated += (results == null ? 0 : results.getNumberRows());
			}
		}
		
		return rowsUpdated;
	}
	
	
	/**
	 * Clears all the values within a given column in this cache container. 
	 * 
	 * 	@param columnName - name of column to clear
	 * 	@throws CDataGridException 
	 */
	public void clearColumn(String columnName)
		throws CDataGridException
	{
		setColumnValue(columnName, null);
	}
		
	/**
	 * Sets column to some value, for all available rows. 
	 * 
	 * @param columnName
	 * @param columnValue
	 * @throws CDataGridException
	 */
	public void setColumnValue(String columnName, Object columnValue)
		throws CDataGridException
	{
		//	Check column existence before we modify anything. 
		if (columnName == null || columnName.trim().length() < 1)
			throw new CDataGridException("Column name cannot be null or empty.");
		if (!metaData.containsColumn(columnName))
			throw new CDataGridException("Column name: " + columnName + " does not exist in this data container.");

		//	Retrieve index of column
		int columnIndex = metaData.getColumnIndex(columnName);

		synchronized (lock)
		{
			CDataRow[] rows = new CDataRow[dataRowMap.size()];
			dataRowMap.values().toArray(rows);
			
			//	Set all values within column
			for (int i = 0; i < rows.length; i++)
				rows[i].setValue(columnIndex, columnValue);		
		}
	}
	
	
	/**
	 *	Adds a unique index to the data cache 
	 *	(this should only be done once in the lifetime of the cache, per column)  
	 *
	 *	@param columnName - the name of the column to add an index on 
	 *	@throws CDataGridException - if index creation failed, or if index on this column already created
	 */		
	public void addUniqueIndex(String columnName)
		throws CDataGridException 
	{
		//	Index already exists 
		if (indexMap.containsKey(columnName))
			return;

		//	Column DNE 
		if (!metaData.containsColumn(columnName))
			throw new CDataGridException("Invalid column name: " + columnName + ".  Cannot build index.");

		//	Add index 
		// System.out.println("Indices not implemented for now...");
	}


	/**
	 *	Adds a non-unique index to the data cache
	 *	(this should only be done once in the lifetime of the cache, per column)
	 *
	 *	@param columnName - name of column to add index on 
	 *	@throws CDataGridException - if index creation failed, or if index on this column already created
	 */
	public void addNonUniqueIndex(String columnName)
		throws CDataGridException 
	{
		//	Index already exists 
		if (indexMap.containsKey(columnName))
			return;

		//	Column DNE 
		if (!metaData.containsColumn(columnName))
			throw new CDataGridException("Invalid column name: " + columnName + ".  Cannot build index.");
		
		//	Create index, and add to map.
		CDataCacheNonUniqueIndex index = new CDataCacheNonUniqueIndex(columnName, metaData.getColumnIndex(columnName));
		CDataRow[] allRows = getAllRows();
		index.index(allRows);
		
		//	Add index to map of indices in container
		indexMap.put(columnName, index);
	}
	
	
	/**
	 * Performs a match based on equality with the primary key field; 
	 * All matches will be returned. 
	 * 
	 * @param values
	 * @return rows of values that match the primary key values passed 
	 */
	public CDataRow[] getPrimaryKeyMatches(Object[] values)
	{
		//	Construct list of all matching rows 
		List list = new LinkedList(); 
		for (int i = 0; i < values.length; i++)
		{
			if (values[i] == null)
				continue;
			
			//	Map / primary key lookup by values, put into results list. 
			CDataRow row = (CDataRow) dataRowMap.get(values[i]);
			if (row != null)
				list.add(row);
		}		
			
		CDataRow[] rows = new CDataRow[list.size()];
		list.toArray(rows);
		return rows;
	}
	
	
	/**
	 * Returns CDataCacheIndex object with a given column name 
	 * 
	 */
	public CDataCacheIndex getCacheIndexByColumnName(String columnName)
	{
		if (indexMap == null)
			return null;
		return (CDataCacheIndex) indexMap.get(columnName);
	}
	
	/**
	 * Columns with indices.
	 * @return
	 */
	public String[] getIndexColumnNames() 
	{
		if (indexMap == null) {
			return new String[0];
		}
		String[] indexColNames = new String[indexMap.size()];
		indexMap.keySet().toArray(indexColNames);
		return indexColNames;
	}
	
	/**
	 *	This method is called after data has been added or removed from the
	 *	cache.  This method effectively rebuilds all configured indices in 
	 *	the cache container.
	 *
	 *	@throws CDataGridException 
	 */
	public void updateIndices()
		throws CDataGridException
	{
		//	No need to re-build indices 
		if (indexMap == null || indexMap.size() < 1)
			return;
	
		//	Iterate through all indices, perform full re-build. 
		CDataRow[] allRows = getAllRows();
		for (Iterator iter = indexMap.keySet().iterator(); iter.hasNext(); ) {
			CDataCacheIndex index = (CDataCacheIndex) iter.next();
			index.update(allRows);
		}
		
		// 	System.out.println("CDataCacheContainer :: Indices not implemented for now...");
	}

	/**
	 * Initialize transient lock object, as deserialization will not 
	 * re-create the lock object.  
	 */
	private synchronized void checkLock() 
	{
		if (lock == null)
			lock = new Object();
	}
	
	/**
	 *	Outputs contents of the cache. 
	 *	@return string representation of object 
	 */
	public String toString()
	{
		StringBuffer sbuf = new StringBuffer();

		try
		{
			sbuf.append("CACHE CONTENTS: ").append(cacheName).append(" (Cardinality: ").append(dataRowMap.size()).append(") ").append("\n");
			sbuf.append(metaData.toString());
			for (Iterator iter = dataRowMap.keySet().iterator(); iter.hasNext();) {
				Object key = iter.next();
				CDataRow row = (CDataRow) dataRowMap.get(key);
				sbuf.append(row.toString()).append("\n");
			}
		}
		catch (Exception ex)
		{
			System.out.println("CDataCacheContainer: " + ex.toString());
			System.out.println(CDataGridException.getStackTraceAsString(ex));
		}

		return sbuf.toString();
	}


	
}

