package net.casper.ext;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.casper.data.model.CDataCacheContainer;
import net.casper.data.model.CDataGridException;
import net.casper.data.model.CDataRowSet;
import net.casper.data.model.CExporter;
import net.casper.data.model.CMarkedUpRow;
import net.casper.data.model.CMarkedUpRowBean;
import net.casper.data.model.CRowMetaData;
import net.casper.io.file.out.CExportCSV;

import org.omancode.rmt.cellreader.narrow.TypeCheckedValue;

/**
 * Utility methods for working with Casper datasets.
 * 
 * @author Oliver Mannion
 * @version $Revision: 201 $
 */
public final class CasperUtil {

	private CasperUtil() {
		// static utility class
	}

	/**
	 * Convert a container column into a map, keyed by another column.
	 * 
	 * @param <M>
	 *            a map containing an object key and object value
	 * @param source
	 *            source casper container
	 * @param map
	 *            map to fill
	 * @param keyColumn
	 *            column that forms the keys of the map
	 * @param valueColumn
	 *            values column that forms the values of the map
	 * @return the map
	 * @throws CDataGridException
	 *             if problem reading {@code source}.
	 */
	public static <M extends Map<Object, Object>> M convertColumnsToMap(
			CDataCacheContainer source, M map, String keyColumn,
			String valueColumn) throws CDataGridException {

		CDataRowSet rowset = source.getAll();

		while (rowset.next()) {
			map.put(rowset.getObject(keyColumn),
					rowset.getObject(valueColumn));
		}

		return map;
	}

	/**
	 * Return a column of a Casper dataset as a double array.
	 * 
	 * @param source
	 *            container
	 * @param column
	 *            column name
	 * @return double array
	 * @throws CDataGridException
	 *             if value in {@code column} is not double
	 */
	public static double[] loadDoubleArray(CDataCacheContainer source,
			String column) throws CDataGridException {
		double[] array = new double[source.size()];

		CDataRowSet rowset = source.getAll();

		int i = 0;

		while (rowset.next()) {
			array[i++] = rowset.getDouble(column);
		}

		return array;
	}

	/**
	 * Return a column of a Casper dataset as a int array.
	 * 
	 * @param source
	 *            container
	 * @param column
	 *            column name
	 * @return double array
	 * @throws CDataGridException
	 *             if value in {@code column} is not double
	 */
	public static int[] loadIntArray(CDataCacheContainer source, String column)
			throws CDataGridException {
		int[] array = new int[source.size()];

		CDataRowSet rowset = source.getAll();

		int i = 0;

		while (rowset.next()) {
			array[i++] = rowset.getInt(column);
		}

		return array;
	}

	/**
	 * Write a casper dataset container to a CSV file.
	 * 
	 * @param fileName
	 *            file name
	 * @param container
	 *            casper container
	 * @throws IOException
	 *             if problem writing container to the file
	 */
	public static void writeToCSV(String fileName,
			CDataCacheContainer container) throws IOException {

		CExporter exporter = new CExportCSV(new File(fileName));

		try {
			container.export(exporter);
		} catch (CDataGridException e) {
			throw new IOException(e);
		}

	}

	/**
	 * Creates a collection of {@link CMarkedUpRowBean}s from a casper
	 * container.
	 * 
	 * @param <E>
	 *            type of bean class. Must implement {@link CMarkedUpRowBean}.
	 * @param source
	 *            casper container source
	 * @param beanClass
	 *            the bean class. Must implement {@link CMarkedUpRowBean}.
	 * @return a collections of newly created {@link CMarkedUpRowBean}s which
	 *         have {@link CMarkedUpRowBean#setMarkedUpRow(CMarkedUpRow)} called
	 *         with the rows from {@code source}
	 * @throws CDataGridException
	 *             if problems reading {@code source} or creating beans.
	 */
	public static <E extends CMarkedUpRowBean> Collection<E> exportMarkedUpRowBeans(
			CDataCacheContainer source, Class<E> beanClass)
			throws CDataGridException {
		Collection<E> beans = new LinkedList<E>();
		CRowMetaData metadata = source.getMetaDefinition();

		CDataRowSet rowset = source.getAll();

		while (rowset.next()) {
			// create new bean
			E bean;
			try {
				bean = beanClass.newInstance();
			} catch (InstantiationException e) {
				throw new CDataGridException(e.getMessage(), e);
			} catch (IllegalAccessException e) {
				throw new CDataGridException(e.getMessage(), e);
			}

			// create row
			CMarkedUpRow mrow =
					new CMarkedUpRow(rowset.getCurrentRow(), metadata); // NOPMD

			bean.setMarkedUpRow(mrow);

			// add bean to collection
			beans.add(bean);
		}

		return beans;
	}

	/**
	 * Scale (ie: multiply) all the numeric values of a casper dataset by a
	 * factor. Non-numeric columns are ignored.
	 * 
	 * @param container
	 *            source dataset
	 * @param factor
	 *            factor to scale by
	 * @return scaled dataset result
	 * @throws CDataGridException
	 *             if problem reading values
	 */
	public static CDataCacheContainer scale(CDataCacheContainer container,
			double factor) throws CDataGridException {

		CDataCacheContainer result = cloneContainer(container);

		CRowMetaData crmd = result.getMetaDefinition();
		List<Integer> numericColumnIndices = new ArrayList<Integer>();

		// get all numeric columns
		Class<?>[] coltypes = crmd.getColumnTypes();
		for (int i = 0; i < coltypes.length; i++) {
			Class<?> type = coltypes[i];

			if (type.getSuperclass() == Number.class) {
				numericColumnIndices.add(i);
			}
		}

		CDataRowSet rowset = result.getAll();

		while (rowset.next()) {
			for (Integer i : numericColumnIndices) {
				double value = rowset.getDouble(i);

				if (!isMissingValue(value)) {
					rowset.setValue(i, value * factor);
				}
			}
		}

		return result;
	}

	/**
	 * Clone a casper data cache container.
	 * 
	 * @param source
	 *            source dataset
	 * @return cloned copy
	 * @throws CDataGridException
	 *             if problem opening source
	 */
	public static CDataCacheContainer cloneContainer(
			CDataCacheContainer source) throws CDataGridException {

		return new CDataCacheContainer(new CasperClone(source));
	}

	/**
	 * Returns true if a value is numeric and is missing.
	 * 
	 * @param value
	 *            value to test
	 * @return {@code true} if numeric missing
	 */
	public static boolean isMissingValue(Object value) {
		if (!(value instanceof Number)) {
			return false;
		}

		Number number = (Number) value;

		if ((value instanceof Byte && (number.byteValue() == TypeCheckedValue.MISSING_VALUE_BYTE))
				|| (value instanceof Integer && (number.intValue() == TypeCheckedValue.MISSING_VALUE_INTEGER))
				|| (value instanceof Double && (((Double) value).isNaN() || number
						.doubleValue() == TypeCheckedValue.MISSING_VALUE_DOUBLE))) {
			return true;
		} else {
			return false;
		}

	}

}