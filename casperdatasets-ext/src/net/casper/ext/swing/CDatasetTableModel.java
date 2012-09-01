package net.casper.ext.swing;

import java.io.IOException;

import javax.swing.table.AbstractTableModel;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRow;
import net.casper.data.model.CDataRuntimeException;
import net.casper.data.model.CRowMetaData;
import net.casper.ext.CasperUtil;

/**
 * A Swing GUI TableModel wrapper for a Casper dataset.
 * 
 * @author Oliver Mannion
 * @version $Revision: 206 $
 */
public class CDatasetTableModel extends AbstractTableModel {
	/**
	 * Serialisation ID.
	 */
	private static final long serialVersionUID = 5783574608323931660L;

	/**
	 * Casper dataset name.
	 */
	private final String name;

	/**
	 * Casper dataset's meta-data.
	 */
	private final CRowMetaData crmd;

	/**
	 * Rows of the dataset.
	 */
	private final CDataRow[] rows;

	/**
	 * Flag to display missing values as empty string or not.
	 */
	private final boolean blankMissingValues;

	/**
	 * Flag to determine whether table cells and underlying dataset are
	 * editable.
	 */
	private final boolean editable;

	/**
	 * Construct a table model from the passed in dataset cache, sorting all
	 * rows by primary key ascending, displaying missing values as empty string,
	 * and non-editable.
	 * 
	 * @param cache
	 *            CDataCacheContainer to create table model for.
	 * @throws IOException
	 *             if cache cannot be loaded
	 */
	public CDatasetTableModel(CDataCacheContainer cache) throws IOException {
		this(cache, true, true, false);
	}

	/**
	 * Construct a table model from the passed in dataset cache. Table Models
	 * are constructed from a CDataCacheContainer and not a CDataRowSet because
	 * CDataRowSets are not thread safe.
	 * 
	 * @param cache
	 *            CDataCacheContainer to create table model for.
	 * @param sortByPrimaryKey
	 *            if true sort the rows by primary key ascending, otherwise use
	 *            the cache's default order
	 * @param blankMissingValues
	 *            if true display empty string instead of the missing value
	 * @param editable
	 *            if table and underlying dataset can be edited
	 * @throws IOException
	 *             if cache cannot be loaded
	 */
	public CDatasetTableModel(CDataCacheContainer cache,
			boolean sortByPrimaryKey, boolean blankMissingValues,
			boolean editable) throws IOException {
		this.name = cache.getCacheName();
		this.blankMissingValues = blankMissingValues;
		this.editable = editable;
		crmd = cache.getMetaDefinition();

		if (sortByPrimaryKey && crmd.getPrimaryKeyColumns() != null) {
			try {
				rows =
						cache.getAll(crmd.getPrimaryKeyColumns(), true)
								.getAllRows();
			} catch (CDataGridException e) {
				throw new IOException(e.getMessage(), e);
			}
		} else {
			rows = cache.getAllRows();
		}
	}

	@Override
	public int getColumnCount() {
		return crmd.getColumnCount();
	}

	@Override
	public int getRowCount() {
		return rows.length;
	}

	@Override
	public Object getValueAt(int rowIndex, int columnIndex) {
		try {
			Object value = rows[rowIndex].getValue(columnIndex);

			// display empty string instead of missing values
			if (blankMissingValues && CasperUtil.isMissingValue(value)) {
				return "";
			}

			return value;
		} catch (CDataGridException e) {
			throw new CDataRuntimeException(e.getMessage(), e);
		}
	}

	@Override
	public void setValueAt(Object value, int row, int col) {
		try {
			rows[row].setValue(col, value);
			fireTableCellUpdated(row, col);
		} catch (CDataGridException e) {
			throw new CDataRuntimeException(e.getMessage(), e);
		}

	}

	/**
	 * Get a new dataset from the (modified) rows.
	 * 
	 * @return casper data cache container
	 * @throws CDataGridException
	 *             if problem creating new dataset
	 */
	public CDataCacheContainer getContainer() throws CDataGridException {
		// build new container from rows
		CDataCacheContainer newContainer =
				new CDataCacheContainer(name, crmd);
		newContainer.addData(rows);
		return newContainer;
	}

	@Override
	public String getColumnName(int column) {
		return crmd.getColumnName(column + 1);
	}

	/*
	 * JTable uses this method to determine the default renderer/ editor for
	 * each cell.
	 */
	@Override
	public Class<?> getColumnClass(int columnIndex) {
		try {
			return crmd.getColumnTypeCls(columnIndex);
		} catch (CDataGridException e) {
			throw new CDataRuntimeException(e);
		}
	}

	@Override
	public boolean isCellEditable(int row, int col) {
		return editable;
	}

}