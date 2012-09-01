package net.casper.io.file.def;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.io.file.in.CBuildFromFile;

import org.omancode.rmt.cellreader.CellReader;

import com.google.gson.annotations.SerializedName;

/**
 * Stores meta-data (name, columns, primary keys, cell readers) for a casper
 * dataset that is loaded from a file.
 * 
 * @author Oliver Mannion
 * @version $Revision: 190 $
 */
public class CDataFileDef implements CDataFile {

	/**
	 * Dataset name. When serialized to JSON this field is named {@code name}.
	 */
	private final String name;

	/**
	 * Names of the columns in the dataset. When serialized to JSON this field
	 * is named {@code column_names}.
	 */
	@SerializedName("column_names")
	private final String[] columns;

	/**
	 * The cell readers for each column. When serialized to JSON this field is
	 * named {@code column_types}.
	 */
	@SerializedName("column_types")
	private final CellReader<?>[] cellReaders;

	/**
	 * The names of the columns that form the primary key. When serialized to
	 * JSON this field is named {@code primary_key}.
	 */
	@SerializedName("primary_key")
	private final String[] primaryKey;

	/**
	 * The loaded Casper container.
	 */
	private transient CDataCacheContainer container = null;

	/**
	 * Construct a {@code DatasetDef} without specifying any columns. Column
	 * information will be loaded from the file using the default
	 * {@link org.omancode.rmt.tablereader.Column} cell reader.
	 * 
	 * @param name
	 *            Dataset name.
	 * @param primaryKey
	 *            The names of the columns that form the primary key.
	 */
	public CDataFileDef(String name, String primaryKey) {
		this(name, null, null, primaryKey);
	}

	/**
	 * Construct a {@code DatasetDef}.
	 * 
	 * @param name
	 *            casper container name.
	 * @param columns
	 *            Names of the columns in the dataset, separated by commas. eg:
	 *            "firstname,lastname" (NB: do not include a space after the
	 *            comma).
	 * @param cellReaders
	 *            Cell readers for each column.
	 * @param primaryKey
	 *            The names of the columns that form the primary key, separated
	 *            by commas, eg: "firstname,lastname" (NB: do not include a
	 *            space after the comma).
	 */
	public CDataFileDef(String name, String columns,
			CellReader<?>[] cellReaders, String primaryKey) {
		this(name, columns == null ? null : columns.split(","), cellReaders,
				primaryKey == null ? null : primaryKey.split(","));
	}

	/**
	 * Construct a {@code DatasetDef}.
	 * 
	 * @param name
	 *            casper container name.
	 * @param columns
	 *            Names of the columns in the dataset
	 * @param cellReaders
	 *            Cell readers for each column.
	 * @param primaryKey
	 *            The names of the columns that form the primary key.
	 */
	public CDataFileDef(String name, String[] columns,
			CellReader<?>[] cellReaders, String[] primaryKey) {
		this.name = name;

		this.columns = columns;

		this.primaryKey = primaryKey;
		this.cellReaders = cellReaders;
	}

	@Override
	public CDataCacheContainer loadDataset(File file) throws IOException {

		try {
			container =
					new CDataCacheContainer(new CBuildFromFile(file, name,
							columns, cellReaders, primaryKey));

			return container;
		} catch (CDataGridException e) {
			throw new IOException(e);
		}
	}

	@Override
	public CDataCacheContainer getContainer() {
		return container;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String toString() {
		String newline = System.getProperty("line.separator");
		StringBuffer result = new StringBuffer(512);

		result.append(getClass().getName()).append(newline);

		result.append("Name: ").append(name).append(newline);
		result.append("Columns: ").append(Arrays.toString(columns))
				.append(newline);
		result.append("Cell readers: ").append(Arrays.toString(cellReaders))
				.append(newline);
		result.append("Primary key(s): ").append(Arrays.toString(primaryKey))
				.append(newline);

		return result.toString();

	}
}