package net.casper.io.file.out;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import net.casper.data.model.CExporter;

import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.supercsv.prefs.CsvPreference;

/**
 * Exports a casper dataset to a CSV file.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 */
public class CExportCSV implements CExporter {

	/**
	 * SuperCSV writer.
	 */
	private final ICsvListWriter writer;

	/**
	 * Data set column names to write to the CSV file.
	 */
	private final String[] selectedColumnNames;

	/**
	 * Data set column positions to write to the CSV file.
	 */
	private final Collection<Integer> selectedColumnIndices = new LinkedList<Integer>();

	/**
	 * Construct a casper exporter that writes all columns in CSV format to
	 * {@code file}.
	 * 
	 * @param file
	 *            CSV file to create and write to.
	 * @throws IOException
	 *             if problem creating file.
	 */
	public CExportCSV(File file) throws IOException {
		this(file, null);
	}

	/**
	 * Construct a casper exporter that writes the specified columns in CSV
	 * format to {@code file}.
	 * 
	 * @param file
	 *            CSV file to create and write to.
	 * @param selectedColumnNames
	 *            the columns to write, or {@code null} to write all columns.
	 *            Multiple columns are separated by commas, eg:
	 *            "firstname,lastname" (NB: do not include a space after the
	 *            comma).
	 * @throws IOException
	 *             if problem creating file.
	 */
	public CExportCSV(File file, String selectedColumnNames) throws IOException {
		this.writer = new CsvListWriter(new FileWriter(file),
				CsvPreference.STANDARD_PREFERENCE);
		this.selectedColumnNames = (selectedColumnNames == null) ? null
				: selectedColumnNames.split(",");
	}

	@Override
	public Object close() {
		try {
			writer.close();
		} catch (IOException e) {
			// swallow exception
		}
		return null;
	}

	@Override
	public void setName(String cacheName) throws IOException {
		// nothing to do
	}

	@Override
	public void setColumnNames(String[] columnNames) throws IOException {
		if (selectedColumnNames == null) {
			// write out all columns
			writer.writeHeader(columnNames);

			for (int i = 0; i < columnNames.length; i++) {
				selectedColumnIndices.add(i);
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

			writer.writeHeader(selectedColumnNames);
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

		dataToWrite = new Object[selectedColumnIndices.size()];
		int i = 0;

		// write out only the selected columns
		for (Integer col : selectedColumnIndices) {
			dataToWrite[i++] = (row[col] == null) ? "" : row[col];
		}
		writer.write(dataToWrite);
	}

	@Override
	public void open() throws IOException {
		// nothing to do
	}

}
