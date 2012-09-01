package net.casper.io.joda;

import org.joda.property.Bean;
import org.joda.property.JodaFactory;
import org.joda.property.Property;
import org.joda.property.impl.BeanMetaData;
import org.joda.property.impl.PropertyMetaData;
import org.joda.property.impl.data.CodedBeanPropertyData;
import org.joda.property.type.BooleanPrimitiveProperty;
import org.joda.property.type.BooleanProperty;
import org.joda.property.type.CharPrimitiveProperty;
import org.joda.property.type.DoublePrimitiveProperty;
import org.joda.property.type.DoubleProperty;
import org.joda.property.type.IntegerPrimitiveProperty;
import org.joda.property.type.IntegerProperty;
import org.joda.property.type.StringProperty;
import org.joda.util.Validation;

/**
 * Utility methods for working with Joda beans.
 * 
 * @author Oliver Mannion
 * @version $Revision: 95 $
 */
public final class JodaBeanUtil {

	private JodaBeanUtil() {
		// utility class
	}

	/**
	 * Fill bean's properties with values. Creates virtual properties if they
	 * don't already exist.
	 * 
	 * @param bean
	 *            joda bean
	 * @param propertyNames
	 *            property names
	 * @param propertyTypes
	 *            property types
	 * @param propertyValues
	 *            property values
	 * @return bean the bean passed in. Allows chaining of this function.
	 * @throws PropertyException
	 *             if problem getting existing property, or creating new one.
	 */
	public static Bean fillBean(Bean bean, String[] propertyNames,
			Class<?>[] propertyTypes, Object[] propertyValues)
			throws PropertyException {

		int numProps = propertyNames.length;

		if (numProps != propertyTypes.length
				|| numProps != propertyValues.length) {
			throw new IllegalArgumentException(
					"names.length != types.length != values.length");
		}

		for (int i = 0; i < numProps; i++) {
			Property prop = getPropertyWithCreate(bean, propertyNames[i],
					propertyTypes[i]);
			prop.set(propertyValues[i]);
		}

		return bean;
	}

	/**
	 * Gets the property, or if it doesn't exist, creates it.
	 * 
	 * @param bean
	 *            joda bean
	 * @param name
	 *            property name
	 * @param basicType
	 *            type of property. If property exists, used to check property
	 *            type. If property needs to be created, used to create property
	 *            of this type.
	 * @return Property
	 * @throws PropertyException
	 *             If property exists but is not of type {@code basicType}, or
	 *             if {@code basicType} is not supported.
	 */
	public static Property getPropertyWithCreate(Bean bean, String name,
			Class<?> basicType) throws PropertyException {

		org.joda.property.Property prop = bean.getProperty(name);

		if (prop == null) {
			return addVirtualProperty(bean, name, basicType);
		} else {
			Class<?> propType = prop.getPropertyType();
			if (!equivalentTypes(propType, basicType)) {

				// incompatible types
				throw new PropertyException("Expected property type "
						+ basicType + " is not compatible with actual type "
						+ prop.getPropertyType() + " for property " + name);
			}

			return prop;
		}
	}

	/**
	 * Compare whether {@code type1} is an equivalent class to {@code type2}.
	 * Types are equivalent if they are the same type, or if one type is a boxed
	 * version of the other primitive type, eg: int.class and Integer.class
	 * 
	 * @param type1
	 *            type1
	 * @param type2
	 *            type2
	 * @return {@code true} if {@code type1} and {@code type2} are equivalent
	 */
	public static boolean equivalentTypes(Class<?> type1, Class<?> type2) {
		if (type1.equals(type2)) {
			return true;
		}

		if (type1 == Byte.class) {
			return type2 == byte.class;
		} else if (type1 == byte.class) {
			return type2 == Byte.class;
		}

		if (type1 == Short.class) {
			return type2 == short.class;
		} else if (type1 == short.class) {
			return type2 == Short.class;
		}

		if (type1 == Integer.class) {
			return type2 == int.class;
		} else if (type1 == int.class) {
			return type2 == Integer.class;
		}

		if (type1 == Long.class) {
			return type2 == long.class;
		} else if (type1 == long.class) {
			return type2 == Long.class;
		}

		if (type1 == Float.class) {
			return type2 == float.class;
		} else if (type1 == float.class) {
			return type2 == Float.class;
		}

		if (type1 == Long.class) {
			return type2 == long.class;
		} else if (type1 == long.class) {
			return type2 == Long.class;
		}

		if (type1 == Double.class) {
			return type2 == double.class;
		} else if (type1 == double.class) {
			return type2 == Double.class;
		}

		if (type1 == Boolean.class) {
			return type2 == boolean.class;
		} else if (type1 == boolean.class) {
			return type2 == Boolean.class;
		}

		if (type1 == Character.class) {
			return type2 == char.class;
		} else if (type1 == char.class) {
			return type2 == Character.class;
		}

		return false;
	}

	/**
	 * Create property on Joda bean.
	 * 
	 * @param bean
	 *            joda bean
	 * @param name
	 *            property name. This is case sensitive if the Joda Bean is
	 *            using case sensitive keys in its Property map, in which case
	 *            "age" and "Age" are two different properties.
	 * @param basicType
	 *            property type.
	 * @return Property.
	 * @throws PropertyException
	 *             if {@code basicType} is not supported.
	 */
	public static Property addVirtualProperty(Bean bean, String name,
			Class<?> basicType) throws PropertyException {

		Class<? extends Property> propertyType = lookupPropertyType(basicType);

		return JodaFactory.getInstance().createProperty(bean, name,
				propertyType, new Class[] { basicType });
	}

	/**
	 * Create a property on a Joda bean from a existing method that returns a
	 * Property object. For example: {@code public StringProperty surname()
	 * return (StringProperty) getProperty("surname"); }
	 * 
	 * @param bean
	 *            joda bean
	 * @param name
	 *            property name
	 * @return the property
	 */
	public static org.joda.property.Property addSoftWiredProperty(Bean bean,
			String name) {
		Validation.isNotNull(name, "Property name");

		return JodaFactory.getInstance().createProperty(bean, name, null);
	}

	/**
	 * Create a property on a Joda bean from existing set/get methods. Throws
	 * NullPointerException if the specified set/get methods do not exist.
	 * 
	 * @param bean
	 *            joda bean
	 * @param name
	 *            the name of the Joda Property to establish on the bean. This
	 *            name is case-sensitive if the Joda Bean is using case
	 *            sensitive keys on its Property Map. This is also the name of
	 *            the get/set methods, however when used to locate the method it
	 *            is case insensitive e.g.: a name of "Age" will create a Joda
	 *            Property named "Age" and will match to "getage" and "getAge".
	 *            Note that the Joda Bean may also carry another separate
	 *            property called "age" if using a Property Map with case
	 *            sensitive keys.
	 * @return the property
	 */
	public static org.joda.property.Property addHardWiredProperty(Bean bean,
			String name) {
		Validation.isNotNull(name, "Property name");

		CodedBeanPropertyData propData = new CodedBeanPropertyData(bean, name);

		return JodaFactory.getInstance().createProperty(bean, name, null,
				new Class[] { propData.getPropertyType() }, propData);
	}

	/**
	 * Given a basic type, return the corresponding Joda Property class.
	 * 
	 * @param basicType
	 *            basic type, e.g.: String, Character, boolean, int, double,
	 *            Boolean, Integer, Double.
	 * @return Class of type Property.
	 * @throws PropertyException
	 *             if {@code basicType} is not supported.
	 */
	public static Class<? extends Property> lookupPropertyType(
			Class<?> basicType) throws PropertyException {
		Class<? extends Property> propType = null;

		if (basicType == String.class) {
			propType = StringProperty.class;
		} else if (basicType == Character.class) {
			propType = CharPrimitiveProperty.class;
		} else if (basicType == boolean.class) {
			propType = BooleanPrimitiveProperty.class;
		} else if (basicType == int.class) {
			propType = IntegerPrimitiveProperty.class;
		} else if (basicType == double.class) {
			propType = DoublePrimitiveProperty.class;
		} else if (basicType == Boolean.class) {
			propType = BooleanProperty.class;
		} else if (basicType == Integer.class) {
			propType = IntegerProperty.class;
		} else if (basicType == Double.class) {
			propType = DoubleProperty.class;
		} else {
			throw new PropertyException("Cannot determine property type for "
					+ basicType);
		}

		return propType;
	}

	/**
	 * Inspect Joda bean class and print list of properties.
	 * 
	 * @param beanClass
	 *            class that implements Bean interface.
	 */
	public static void printBeanProperties(Class<? extends Bean> beanClass) {

		BeanMetaData bmd = BeanMetaData.getMetaData(beanClass);

		System.out.println("Bean class " + beanClass
				+ " has the following properties:");

		for (PropertyMetaData pmd : bmd.getPropertyMetaData()) {
			System.out.println(pmd.getPropertyName());
		}
	}

}
