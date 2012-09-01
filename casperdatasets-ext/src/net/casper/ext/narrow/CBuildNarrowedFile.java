package net.casper.ext.narrow;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import net.casper.data.model.CBuilder;
import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.io.file.in.CBuildFromFile;

/**
 * Convenience builder that loads a file using {@link CBuildFromFile} and
 * narrows it at the same time using {@link CBuildNarrowed}.
 * 
 * Useful for applying to files that have been created from sources that have no
 * data type associated with columns (e.g. CSV files) or loosely specified data
 * types (eg. Excel files) and the data type can't be specified at the time of
 * loading.
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
public class CBuildNarrowedFile implements CBuilder {

	private CBuildNarrowed narrower;

	/**
	 * Create narrowed delimited file builder using default delimiter settings.
	 * Container name is the file name and will read column header from the file
	 * with no primary key.
	 * 
	 * @param file
	 *            file
	 * @throws IOException
	 *             if IO problem reading file header.
	 */
	public CBuildNarrowedFile(File file) throws IOException {
		this(file, null, null, null);
	}

	/**
	 * Create narrowed delimited file builder using default delimiter settings.
	 * Container name is the file name and will read column header from the
	 * file. Missing values are converted during narrowing.
	 * 
	 * @param file
	 *            file
	 * @param primaryKeys
	 *            {@code null} if no primary key otherwise the column names of
	 *            primary keys, separated by comma, eg: "firstname,lastname" NB:
	 *            do not include a space after the comma.
	 * @throws IOException
	 *             if IO problem reading file header.
	 */
	public CBuildNarrowedFile(File file, String primaryKeys) throws IOException {
		this(file, null, null, (primaryKeys == null) ? null : primaryKeys
				.split(","));
	}

	/**
	 * Construct a narrowed casper delimited file builder.
	 * 
	 * @param file
	 *            file
	 * @param containerName
	 *            if {@code null} will use default (the file name)
	 * @param columnNames
	 *            the name of the columns in the file. If {@code null}, column
	 *            names will be loaded from the file header. If specified then
	 *            the file must have a header with the same columns names (case
	 *            and order insensitive).
	 * @param primaryKeys
	 *            {@code null} if no primary key otherwise an array of primary
	 *            key names
	 * @throws IOException
	 *             if IO problem reading file header.
	 */
	public CBuildNarrowedFile(File file, String containerName,
			String[] columnNames, String[] primaryKeys) throws IOException {

		// use default cell readers for the file type
		CBuilder fileBuilder = new CBuildFromFile(file, containerName,
				columnNames, null, primaryKeys);

		try {
			CDataCacheContainer container = new CDataCacheContainer(fileBuilder);
			narrower = new CBuildNarrowed(container);

		} catch (CDataGridException e) {
			throw new IOException(e);
		}

	}

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
	public CBuildNarrowedFile setConvertMissing(boolean convertMissing) {
		narrower.setConvertMissing(convertMissing);
		return this;
	}

	@Override
	public void close() {
		narrower.close();
	}

	@Override
	public String[] getColumnNames() {
		return narrower.getColumnNames();
	}

	@Override
	public Class[] getColumnTypes() {
		return narrower.getColumnTypes();
	}

	@Override
	public Map getConcreteMap() {
		return narrower.getConcreteMap();
	}

	@Override
	public String getName() {
		return narrower.getName();
	}

	@Override
	public String[] getPrimaryKeyColumns() {
		return narrower.getPrimaryKeyColumns();
	}

	@Override
	public void open() throws IOException {
		narrower.open();
	}

	@Override
	public Object[] readRow() throws IOException {
		return narrower.readRow();
	}

}
