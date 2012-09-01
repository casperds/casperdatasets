package net.casper.io.joda;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import net.casper.data.model.CBuilder;

import org.joda.property.Bean;
import org.joda.property.Property;

/**
 * Builds a data cache from a Collection of Joda beans. The Property Map is used
 * to determine the bean properties and each one becomes a column in the data
 * cache.
 * 
 * @author Oliver Mannion
 * @version $Revision: 95 $
 * @param <E>
 *            Collection's element type
 */
public class CBuildFromJodaBeans<E extends Bean> implements CBuilder {

	private final Collection<E> source;
	private final Collection<String> propNames = new LinkedList<String>();
	private final Collection<Class<?>> propTypes = new LinkedList<Class<?>>();
	private int numProps = 0;
	private final String[] primaryKeyPropNames;
	private final Iterator<E> iterator;
	private final String name;
	private boolean reportBoxedPrimitiveType = true;

	/**
	 * Builds a data cache from the given Collection of Joda Beans.
	 * 
	 * @param name
	 *            the name of the data cache to create.
	 * @param source
	 *            the Java collection to convert.
	 * @param primaryKeyPropNames
	 *            property names of the primary key(s)
	 */
	public CBuildFromJodaBeans(String name, Collection<E> source,
			String[] primaryKeyPropNames) {
		this.name = name;
		this.source = source;
		this.iterator = source.iterator();
		this.primaryKeyPropNames = primaryKeyPropNames;
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
	public CBuildFromJodaBeans<E> setReportBoxedType(
			boolean reportBoxedPrimitive) {
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
		E bean = source.iterator().next();

		Map propertyMap = bean.getPropertyMap();

		for (Object o : propertyMap.entrySet()) {
			Map.Entry<String, Property> entry = (Map.Entry<String, Property>) o;

			propNames.add(entry.getKey());
			numProps++;

			if (reportBoxedPrimitiveType) {
				propTypes.add(boxedPrimitiveType(entry.getValue()
						.getPropertyType()));
			} else {
				propTypes.add(entry.getValue().getPropertyType());
			}
		}
	}

	/**
	 * Returns the boxed equivalent of a primitive type class. If the supplied
	 * class is not a primitive class, returns it as is.
	 * 
	 * @param primitiveClass
	 *            a primitive type class, eg: int.
	 * @return boxed version of the primitive type, eg: if primitiveType is int,
	 *         returns java.lang.Integer.
	 */
	public Class<?> boxedPrimitiveType(Class<?> primitiveClass) {

		if (primitiveClass == byte.class) {
			return Byte.class;
		} else if (primitiveClass == short.class) {
			return Short.class;
		} else if (primitiveClass == int.class) {
			return Integer.class;
		} else if (primitiveClass == long.class) {
			return Long.class;
		} else if (primitiveClass == float.class) {
			return Float.class;
		} else if (primitiveClass == double.class) {
			return Double.class;
		} else if (primitiveClass == boolean.class) {
			return Boolean.class;
		} else if (primitiveClass == char.class) {
			return Character.class;
		}
		return primitiveClass;
	}

	@Override
	public Object[] readRow() throws IOException {
		if (!iterator.hasNext()) {
			return null;
		}

		E bean = iterator.next();

		Object[] row = new Object[numProps];

		int i = 0;

		for (String propName : propNames) {
			row[i++] = bean.getProperty(propName).toObject();
		}

		return row;
	}

}
