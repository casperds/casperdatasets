package net.casper.ext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRow;
import net.casper.data.model.CDataRowSet;
import net.casper.data.model.CRowMetaData;

/**
 * Builds a data cache from an existing one. Can be used to clone a casper data
 * cache container.
 * 
 * @author Oliver Mannion
 * @version $Revision: 201 $
 * 
 */
public class CasperClone implements CBuilder {

	private final CDataCacheContainer source;
	private final CRowMetaData meta;
	private final CDataRowSet rowset;

	/**
	 * Create clone builder.
	 * 
	 * @param source
	 *            source container.
	 * @throws CDataGridException
	 *             if problem opening
	 */
	public CasperClone(CDataCacheContainer source) throws CDataGridException {
		this.source = source;
		this.meta = source.getMetaDefinition();
		this.rowset = source.getAll();
	}

	@Override
	public String getName() {
		return source.getCacheName();
	}

	@Override
	public String[] getColumnNames() {
		return meta.getColumnNames();
	}

	@Override
	public Class[] getColumnTypes() {
		return meta.getColumnTypes();
	}

	@Override
	public String[] getPrimaryKeyColumns() {
		return meta.getPrimaryKeyColumns();
	}

	@Override
	public Map getConcreteMap() {
		return new HashMap();
	}

	@Override
	public void open() throws IOException {
		// do nothing
	}

	@Override
	public Object[] readRow() throws IOException {
		try {
			if (!rowset.next()) {
				return null;
			}

			CDataRow row = rowset.getCurrentRow();
			Object[] rowdata = row.getRawData();
			return rowdata.clone();

		} catch (CDataGridException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	@Override
	public void close() {
		// do nothing
	}

}
