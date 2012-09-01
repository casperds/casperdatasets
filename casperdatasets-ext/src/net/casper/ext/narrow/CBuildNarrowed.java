package net.casper.ext.narrow;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.data.model.CRowMetaData;

import org.omancode.rmt.cellreader.narrow.NarrowException;
import org.omancode.rmt.cellreader.narrow.NarrowUtil;

/**
 * Narrows a casper cache or dataset. For each column determines the narrowest
 * possible data type that can be applied to every item in the column without
 * loss of fidelity. Then returns each row with its data converted to the
 * narrowest type.
 * 
 * Useful for applying to datasets that have been created from sources that have
 * no data type associated with columns (e.g. CSV files) or loosely specified
 * data types (eg. Excel files) and the data type can't be specified at the time
 * of loading.
 * 
 * Missing integer or double values will be treated as Strings, unless
 * {@link #setConvertMissing(boolean)} is {@code true} in which case they will
 * be converted to
 * {@link org.omancode.rmt.cellreader.CellReaders#MISSING_VALUE_INTEGER} or
 * {@link org.omancode.rmt.cellreader.CellReaders#MISSING_VALUE_DOUBLE} instead.
 * 
 * @author Oliver Mannion
 * @version $Revision: 181 $
 */
public class CBuildNarrowed implements CBuilder {

	private final CDataCacheContainer container;

	private CDataRowSet rowset = null;

	private Class<?>[] narrowedColumnTypes = null;

	/**
	 * Use {@link org.omancode.rmt.cellreader.CellReaders#OPTIONAL_DOUBLE} and
	 * {@link org.omancode.rmt.cellreader.CellReaders#OPTIONAL_INTEGER}?
	 */
	private boolean convertMissing = false;

	/**
	 * If set, then missing integers and doubles will be returned as
	 * {@link org.omancode.rmt.cellreader.CellReaders#MISSING_VALUE_INTEGER} or
	 * {@link org.omancode.rmt.cellreader.CellReaders#MISSING_VALUE_DOUBLE}
	 * instead.
	 * 
	 * @param convertMissing
	 *            true to convert missing ints/doubles
	 * @return this
	 */
	public CBuildNarrowed setConvertMissing(boolean convertMissing) {
		this.convertMissing = convertMissing;
		return this;
	}

	/**
	 * Constructor for narrowing casper cache. Narrowed cache has same name as
	 * source.
	 * 
	 * @param source
	 *            casper cache
	 * @throws CDataGridException
	 *             if problems reading source.
	 */
	public CBuildNarrowed(CDataCacheContainer source) throws CDataGridException {
		this.container = source;
	}

	/**
	 * Constructor for narrowing casper rowset. Constructs a new casper cache
	 * from the (non thread-safe) rowset.
	 * 
	 * @param cacheName
	 *            name of narrowed cache
	 * @param source
	 *            casper rowset
	 * @throws CDataGridException
	 *             if problems reading source.
	 */
	public CBuildNarrowed(String cacheName, CDataRowSet source)
			throws CDataGridException {

		// build new container from rowset
		CDataCacheContainer newContainer = new CDataCacheContainer(cacheName,
				source.getMetaDefinition());
		newContainer.addData(source.getAllRows());
		this.container = newContainer;
	}

	@Override
	public void open() throws IOException {
		try {

			rowset = container.getAll();
			narrowedColumnTypes = calcNarrowColTypes(rowset);
			rowset.beforeFirst();

		} catch (CDataGridException e) {
			throw new IOException(e);
		}

	}

	/**
	 * Determine narrowest type for each column in the rowset.
	 * 
	 * @param rowset
	 *            rowset
	 * @return narrowest type for column
	 * @throws CDataGridException
	 *             if problem reading rowset.
	 */
	private Class<?>[] calcNarrowColTypes(CDataRowSet rowset)
			throws CDataGridException {

		CRowMetaData dsmeta = rowset.getMetaDefinition();
		int cols = dsmeta.getColumnCount();

		Class<?>[] narrowType = new Class[cols];
		int col = 0;

		String[] columnNames = dsmeta.getColumnNames();
		for (String columnName : columnNames) {

			Object[] columnValues = rowset.getColumnValues(columnName);
			narrowType[col++] = NarrowUtil.calcNarrowestType(columnValues,
					convertMissing);

		}

		return narrowType;
	}

	@Override
	public String getName() {
		return container.getCacheName();
	}

	@Override
	public String[] getColumnNames() {
		return container.getMetaDefinition().getColumnNames();
	}

	@Override
	public Class[] getColumnTypes() {
		return narrowedColumnTypes;
	}

	@Override
	public Map getConcreteMap() {
		// use a LinkedHashMap to retain insertion order
		return new LinkedHashMap();
	}

	@Override
	public String[] getPrimaryKeyColumns() {
		return container.getMetaDefinition().getPrimaryKeyColumns();
	}

	@Override
	public Object[] readRow() throws IOException {
		try {
			if (!rowset.next()) {
				return null;
			}

			// get current row and convert to narrow types
			Object[] row = rowset.getCurrentRow().getRawData();

			return NarrowUtil.narrowArray(row, narrowedColumnTypes,
					convertMissing);

		} catch (CDataGridException e) {
			throw new IOException(e);
		} catch (NarrowException e) {
			throw new IOException(e);
		}
	}

	@Override
	public void close() {
		// nothing to do
	}

}
