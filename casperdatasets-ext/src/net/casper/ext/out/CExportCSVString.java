package net.casper.ext.out;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import net.casper.data.model.CExporter;

/**
 * Exports a casper dataset to a CSV string. Useful for comparing datasets.
 * 
 * @author Oliver Mannion
 * @version $Revision: 154 $
 */
public class CExportCSVString implements CExporter {

	private final StringBuffer output = new StringBuffer(1024 * 4);

	private static final String NEWLINE =
			System.getProperty("line.separator");

	/**
	 * Data set column names to write to the CSV file.
	 */
	private final String[] selectedColumnNames;

	/**
	 * Data set column positions to write to the CSV file.
	 */
	private final Collection<Integer> selectedColumnIndices =
			new LinkedList<Integer>();

	/**
	 * Write out the header at the beginning of the string.
	 */
	private final boolean includeHeader;

	/**
	 * Construct a casper exporter that writes out the header and all columns in
	 * CSV format to a string.
	 */
	public CExportCSVString() {
		this(true);
	}

	/**
	 * Construct a casper exporter that writes out all columns in CSV format to
	 * a string.
	 * 
	 * @param includeHeader
	 *            write out the header at the beginning of the string
	 */
	public CExportCSVString(boolean includeHeader) {
		this(includeHeader, null);
	}

	/**
	 * Construct a casper exporter that writes the specified columns in CSV
	 * format to a string.
	 * 
	 * @param selectedColumnNames
	 *            the columns to write, or {@code null} to write all columns.
	 *            Multiple columns are separated by commas, eg:
	 *            "firstname,lastname" (NB: do not include a space after the
	 *            comma).
	 * @param includeHeader
	 *            write out the header at the beginning of the string
	 */
	public CExportCSVString(boolean includeHeader, String selectedColumnNames) {
		this.selectedColumnNames =
				(selectedColumnNames == null) ? null : selectedColumnNames
						.split(",");
		this.includeHeader = includeHeader;
	}

	@Override
	public Object close() {
		return getCSVString();
	}

	@Override
	public void setName(String cacheName) throws IOException {
		// nothing to do
	}

	@Override
	public void setColumnNames(String[] columnNames) throws IOException {
		if (selectedColumnNames == null) {

			if (includeHeader) {
				// write out all columns
				write(columnNames);
			}

		} else {
			// columns to write out have been specified

			for (String colToWrite : selectedColumnNames) {

				// determine the array index of the
				// column that will be written
				int positionOfColToWrite = -1;
				for (int i = 0; i < columnNames.length; i++) {
					if (colToWrite.equalsIgnoreCase(columnNames[i])) {
						positionOfColToWrite = i;
						break;
					}
				}

				if (positionOfColToWrite == -1) {
					throw new IOException("Column named " + colToWrite
							+ " does not exist in column list.");
				}

				selectedColumnIndices.add(positionOfColToWrite);

			}

			if (includeHeader) {
				write(selectedColumnNames);
			}
		}
	}

	@Override
	public void setColumnTypes(Class[] columnTypes) throws IOException {
		// nothing to do
	}

	@Override
	public void setPrimaryKeyColumns(String[] primaryKeyColumns)
			throws IOException {
		// nothing to do
	}

	@Override
	public void writeRow(Object[] row) throws IOException {
		Object[] dataToWrite;

		if (selectedColumnNames == null) {
			// write out all columns
			dataToWrite = row;

		} else {
			dataToWrite = new Object[selectedColumnIndices.size()];
			int i = 0;

			// write out only the selected columns
			for (Integer col : selectedColumnIndices) {
				dataToWrite[i++] = row[col];
			}

		}

		write(dataToWrite);
	}

	private void write(Object[] content) {
		int lastItem = content.length - 1;

		for (int i = 0; i < lastItem; i++) {
			output.append(content[i].toString()).append(", ");
		}
		output.append(content[lastItem]).append(NEWLINE);
	}

	@Override
	public void open() throws IOException {
		// nothing to do
	}

	/**
	 * Return the CSV string. Call this after the export.
	 * 
	 * @return CSV string of the container
	 */
	public String getCSVString() {
		return output.toString();
	}
}