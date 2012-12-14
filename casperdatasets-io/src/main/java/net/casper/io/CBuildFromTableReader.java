package net.casper.io;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import net.casper.data.model.CBuilder;

import org.omancode.rmt.tablereader.AbstractTableReader;
import org.omancode.util.ArrayUtil;

/**
 * Builds casper data using a {@link AbstractTableReader}.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 */
public class CBuildFromTableReader implements CBuilder {

	private final String[] primaryKeys;
	private final AbstractTableReader tblreader;
	private final String[] columnsRead;
	private final Class<?>[] columnTypes;
	private final String containerName;
	private final Iterator<Object[]> rows;

	/**
	 * Create table builder. Container name is the table name and will read
	 * column header from the table and load all columns using the default cell
	 * readers.
	 * 
	 * @param tblreader
	 *            table reader
	 * @param primaryKeys
	 *            column names of primary keys, separated by comma, eg:
	 *            "firstname,lastname" NB: do not include a space after the
	 *            comma.
	 * @throws IOException
	 *             if IO problem reading table header.
	 */
	public CBuildFromTableReader(AbstractTableReader tblreader,
			String primaryKeys) throws IOException {
		this(tblreader, null, primaryKeys.split(","));
	}

	/**
	 * Construct a casper table builder.
	 * 
	 * @param tblreader
	 *            table reader
	 * @param containerName
	 *            if {@code null} will use default (the table name)
	 * @param primaryKeys
	 *            those keys that can be used as lookup keys in the returned
	 *            casper dataset
	 * @throws IOException
	 *             if IO problem reading table header.
	 */
	public CBuildFromTableReader(AbstractTableReader tblreader,
			String containerName, String[] primaryKeys) throws IOException {
		this.tblreader = tblreader;

		this.containerName = containerName == null ? tblreader.getName()
				: containerName;

		this.columnsRead = tblreader.getColumnsRead();

		this.primaryKeys = checkPrimaryKeys(primaryKeys, this.columnsRead);

		try {
			this.rows = tblreader.iterator();
		} catch (RuntimeException e) {
			throw new IOException(e);
		}

		this.columnTypes = tblreader.getColumnTypes();

	}

	/**
	 * Check primary keys, ie: if primary keys are specified make sure they
	 * exist in in the table header.
	 * 
	 * @param specifiedPKs
	 *            primary keys expected
	 * @param columnsAvailable
	 *            columns read in the table
	 * @return clone of specifiedPKs if checks pass
	 * @throws IOException
	 *             if problem with primary keys
	 */
	private String[] checkPrimaryKeys(String[] specifiedPKs,
			String[] columnsAvailable) throws IOException {

		if (specifiedPKs == null) {
			return null;
		}

		// check the primary key columns exist in the table
		String missingPK = ArrayUtil.firstStringComplement(columnsAvailable,
				specifiedPKs, false);
		if (missingPK != null) {
			throw new IOException("Primary key column \"" + missingPK
					+ "\" does not exist/not read from " + tblreader.getName());

		}

		return specifiedPKs.clone();

	}

	@Override
	public void open() throws IOException {
		// nothing to do
	}

	@Override
	public void close() {
		// nothing to do
	}

	@Override
	public String getName() {
		return containerName;
	}

	@Override
	public String[] getColumnNames() {
		return columnsRead;
	}

	@Override
	public Class[] getColumnTypes() {
		return columnTypes.clone();
	}

	@Override
	public Map getConcreteMap() {
		// if no primary key return a LinkedHashMap so the container
		// will be ordered according to insert order, ie:
		// same order in the table
		return (primaryKeys == null) ? new LinkedHashMap() : new HashMap();
	}

	@Override
	public String[] getPrimaryKeyColumns() {
		return primaryKeys;
	}

	@Override
	public Object[] readRow() throws IOException {
		if (!rows.hasNext()) {
			return null;
		}

		return rows.next();
	}

}