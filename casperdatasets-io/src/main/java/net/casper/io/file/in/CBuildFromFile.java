package net.casper.io.file.in;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Locale;
import java.util.Map;

import javax.swing.filechooser.FileFilter;

import net.casper.data.model.CBuilder;
import net.casper.io.CBuildFromTableReader;

import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.tablereader.AbstractTableReader;
import org.omancode.rmt.tablereader.file.DelimitedFileReader;
import org.omancode.rmt.tablereader.file.ExcelFileReader;
import org.omancode.util.io.ExtFileFilter;

/**
 * Loads a casper dataset from a file based on the file type. The type of the
 * file is determined by its extension, and the appropriate file builder is
 * used. Currently supported file types are XLS, XLSX, CSV.
 * 
 * Types of columns in files may be specified or unspecified (in which case the
 * widest possible type is used for loading values, eg: Object or String).
 * Column names may be specified (allowing reading of only certain columns from
 * a file) or unspecified (in which case all columns names do not need to be
 * known in advance and all columns are loaded). Missing values are supporting
 * in files via the use of optional cell readers.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 */
public class CBuildFromFile implements CBuilder {

	private final CBuilder builder;

	/**
	 * Create file builder. Container name is the file name and will read column
	 * header from the file and load all columns. Columns will be of whatever
	 * type is provided by the underlying file reader. No primary key column.
	 * 
	 * @param file
	 *            file
	 * @throws IOException
	 *             if IO problem reading file header.
	 */
	public CBuildFromFile(File file) throws IOException {
		this(file, null);
	}

	/**
	 * Create file builder. Container name is the file name and will read column
	 * header from the file and load all columns. Columns will be of whatever
	 * type is provided by the underlying file reader.
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
	public CBuildFromFile(File file, String primaryKeys) throws IOException {
		this(file, null, null, null, (primaryKeys == null) ? null : primaryKeys
				.split(","));
	}

	/**
	 * Construct a casper file builder.
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
	 * @param cellReaders
	 *            cell readers for each column. If {@code null}, the default
	 *            cell readers for the file type will be used (ie: type returned
	 *            is determined by the underlying file reader).
	 * @param primaryKeys
	 *            {@code null} if no primary key otherwise an array of primary
	 *            key names
	 * @throws IOException
	 *             if IO problem reading file header.
	 */
	public CBuildFromFile(File file, String containerName,
			String[] columnNames, CellReader<?>[] cellReaders,
			String[] primaryKeys) throws IOException {

		if (!file.exists()) {
			throw new FileNotFoundException(file + " does not exist.");
		}

		String extension = getExtension(file);

		if (extension == null) {
			throw new UnsupportedFileTypeException(file
					+ " does not have an extension."
					+ " Type cannot be determined.");
		}

		FileTypeFactories filetype = null;

		// get the factory used to create a builder for
		// this file type. The factory is determined by the file's
		// extension, which will lookup from the FileTypeFactory enum
		try {
			filetype = FileTypeFactories.valueOf(extension.toUpperCase(Locale
					.getDefault()));
		} catch (IllegalArgumentException e) {
			throw new UnsupportedFileTypeException("Loading files of type "
					+ extension + " not supported. Cannot load " + file);
		}

		builder = filetype.getFactory().newBuilder(file, containerName,
				columnNames, cellReaders, primaryKeys);

	}

	@Override
	public void open() throws IOException {
		builder.open();
	}

	@Override
	public void close() {
		builder.close();
	}

	@Override
	public String getName() {
		return builder.getName();
	}

	@Override
	public String[] getColumnNames() {
		return builder.getColumnNames();
	}

	@Override
	public Class[] getColumnTypes() {
		return builder.getColumnTypes();
	}

	@Override
	public Map getConcreteMap() {
		return builder.getConcreteMap();
	}

	@Override
	public String[] getPrimaryKeyColumns() {
		return builder.getPrimaryKeyColumns();
	}

	@Override
	public Object[] readRow() throws IOException {
		return builder.readRow();
	}

	/**
	 * Get the extension of a file.
	 * 
	 * @param file
	 *            to get extension of.
	 * @return a String containing the file's extension.
	 */
	private static String getExtension(File file) {
		String ext = null;
		String fileName = file.getName();
		int indexOfDot = fileName.lastIndexOf('.');

		if (indexOfDot > 0 && indexOfDot < fileName.length() - 1) {
			ext = fileName.substring(indexOfDot + 1);
		}
		return ext;

	}

	/**
	 * An enumeration of file types with their corresponding factories. The
	 * factory returns an instance of the CBuilder required to load that file
	 * type into a casper container.
	 * 
	 * File types implemented so far are XLS (Excel) and CSV files.
	 * 
	 * @author Oliver Mannion
	 * 
	 */
	public enum FileTypeFactories {

		/**
		 * Excel XLS Files.
		 */
		XLS(new CBuilderFileFactory() {

			@Override
			public CBuilder newBuilder(File file, String name,
					String[] columnNames, CellReader<?>[] cellReaders,
					String[] primaryKeys) throws IOException {

				AbstractTableReader cellfile = new ExcelFileReader(file,
						columnNames, cellReaders);

				return new CBuildFromTableReader(cellfile, name, primaryKeys);
			}
		}),

		/**
		 * Excel XLSX Files.
		 */
		XLSX(new CBuilderFileFactory() {

			@Override
			public CBuilder newBuilder(File file, String name,
					String[] columnNames, CellReader<?>[] cellReaders,
					String[] primaryKeys) throws IOException {

				AbstractTableReader cellfile = new ExcelFileReader(file,
						columnNames, cellReaders);

				return new CBuildFromTableReader(cellfile, name, primaryKeys);
			}
		}),

		/**
		 * CSV files.
		 */
		CSV(new CBuilderFileFactory() {

			@Override
			public CBuilder newBuilder(File file, String name,
					String[] columnNames, CellReader<?>[] cellReaders,
					String[] primaryKeys) throws IOException {

				AbstractTableReader cellfile = new DelimitedFileReader(file,
						columnNames, cellReaders, null);

				return new CBuildFromTableReader(cellfile, name, primaryKeys);
			}
		});

		/**
		 * File Filter representing all file types in this enum.
		 */
		private static FileFilter fileFilter = initializeFileFilter();

		private final CBuilderFileFactory factory;

		private FileTypeFactories(CBuilderFileFactory factory) {
			this.factory = factory;
		}

		/**
		 * Get file type factory.
		 * 
		 * @return file factory.
		 */
		public CBuilderFileFactory getFactory() {
			return factory;
		}

		/**
		 * Return a FileFilter representing all FileTypes that can be used in a
		 * JFileChooser dialog.
		 * 
		 * @return FileFilter representing all FileTypes
		 */
		public static FileFilter getFilter() {
			return fileFilter;
		}

		/**
		 * A FileFilter that represents all FileTypeFactorys in the enum.
		 * 
		 * @return FileFilter representing all FileTypeFactorys
		 */
		private static FileFilter initializeFileFilter() {
			ExtFileFilter filter = new ExtFileFilter();

			for (FileTypeFactories ft : FileTypeFactories.values()) {
				filter.addExtension(ft.toString());
			}
			return filter;
		}

	}

}