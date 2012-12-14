package net.casper.io.beans;

import java.beans.IntrospectionException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import net.casper.data.model.CBuilder;

import org.apache.commons.beanutils.PropertyUtils;
import org.omancode.util.beans.BeanPropertyInspector;

/**
 * Builds a data cache from the given Collection. Introspection is used to
 * determine the bean properties (i.e.: getter methods) that are exposed, and
 * each one becomes a column in the data cache.
 * 
 * @author Oliver Mannion
 * @version $Revision: 147 $
 * 
 */
public class CBuildFromCollection implements CBuilder {

	private final String name;
	private final Collection<?> source;
	private Iterator<?> iterator;
	private final String[] primaryKeyPropNames;

	/**
	 * Columns are created for all getter methods that are defined by
	 * {@code stopClass}'s subclasses. {@code stopClass}'s getter methods and
	 * superclass getter methods are not converted to columns in the dataframe.
	 */
	private final Class<?> stopClass;

	private boolean reportBoxedPrimitiveType = true;

	private Collection<String> propNames = new LinkedList<String>();
	private Collection<Class<?>> propTypes = new LinkedList<Class<?>>();
	private int numProps = 0;

	/**
	 * Builds a data cache from the given Collection. Introspection is used to
	 * determine the bean properties (i.e.: getter methods) that are exposed,
	 * and each one becomes a column in the data cache.
	 * 
	 * @param name
	 *            the name of the data cache to create.
	 * @param source
	 *            the Java collection to convert.
	 * @param stopClass
	 *            Columns are created for all getter methods that are defined by
	 *            {@code stopClass}'s subclasses. {@code stopClass}'s getter
	 *            methods and superclass getter methods are not converted to
	 *            columns in the dataframe.
	 * @param primaryKeyPropNames
	 *            property names of the primary key(s), separated by commas, eg:
	 *            "firstname,lastname" (NB: do not include a space after the
	 *            comma), or {@code null} if no primary key.
	 */
	public CBuildFromCollection(String name, Collection<?> source,
			Class<?> stopClass, String primaryKeyPropNames) {
		this.name = name;
		this.source = source;
		this.stopClass = stopClass;

		this.primaryKeyPropNames = (primaryKeyPropNames == null) ? null
				: primaryKeyPropNames.split(",");
	}

	/**
	 * Determine whether primitive bean properties return a primitive type or a
	 * boxed type. By default a boxed type is reported. This doesn't affect the
	 * storing of the value, it only means the CMetaData for the created
	 * container will report a boxed type, eg: java.lang.Integer instead of int.
	 * 
	 * @param reportBoxedPrimitive
	 *            whether bean properties returning a primitive type should
	 *            report these as a boxed type.
	 * @return this. Can be chained.
	 */
	public CBuildFromCollection setReportBoxedType(boolean reportBoxedPrimitive) {
		this.reportBoxedPrimitiveType = reportBoxedPrimitive;
		return this;
	}

	@Override
	public void close() {
		// do nothing
	}

	@Override
	public String[] getColumnNames() {
		return propNames.toArray(new String[numProps]);
	}

	@Override
	public Class<?>[] getColumnTypes() {
		return propTypes.toArray(new Class[numProps]);
	}

	@Override
	public Map getConcreteMap() {
		return new HashMap();
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public String[] getPrimaryKeyColumns() {
		return primaryKeyPropNames;
	}

	@Override
	public void open() throws IOException {
		iterator = source.iterator();
		Object bean = source.iterator().next();

		try {
			// get the property names and types
			BeanPropertyInspector props = new BeanPropertyInspector(bean,
					stopClass, reportBoxedPrimitiveType);

			propTypes = props.getTypes();
			propNames = props.getNames();
			numProps = props.getCount();

		} catch (IntrospectionException e) {
			throw new IOException(e);
		}

	}

	@Override
	public Object[] readRow() throws IOException {
		if (!iterator.hasNext()) {
			return null;
		}

		Object bean = iterator.next();
		Object[] row = new Object[numProps];

		int col = 0;

		for (String propName : propNames) {

			try {
				row[col++] = PropertyUtils.getProperty(bean, propName);
			} catch (IllegalAccessException e) {
				throw new IOException(e);
			} catch (InvocationTargetException e) {
				throw new IOException(e);
			} catch (NoSuchMethodException e) {
				throw new IOException(e);
			}
		}

		return row;
	}

}
