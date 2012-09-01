package net.casper.io.file.def;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;

import org.omancode.rmt.cellreader.CellReader;
import org.omancode.rmt.cellreader.CellReaderException;
import org.omancode.rmt.cellreader.CellReaders;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.InstanceCreator;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * Loads {@link CDataFileDef} instances from JSON strings and text files. Can
 * also serialize {@link CDataFileDef} instances to JSON strings. Example
 * {@link CDataFileDef} JSON string:
 * 
 * <pre>
 * {
 * &quot;name&quot;:&quot;Adults&quot;,
 * &quot;column_names&quot;:[
 *     &quot;updated&quot;,
 *     &quot;sex&quot;,
 *     &quot;weight&quot;,
 *     &quot;age&quot;,
 *     &quot;name&quot;,
 *     &quot;height&quot;,
 *     &quot;children&quot;
 * ],
 * &quot;column_types&quot;:[
 *     &quot;boolean&quot;,
 *     &quot;character&quot;,
 *     &quot;double&quot;,
 *     &quot;integer&quot;,
 *     &quot;string&quot;,
 *     &quot;optional double&quot;,
 *     &quot;optional integer&quot;
 * ],
 * &quot;primary_key&quot;:[
 *     &quot;integer&quot;
 * ]
 * }
 * </pre>
 * 
 * {@link CellReader}s are mapped to and from strings using the mappings
 * provided by {@link CellReaders}, ie: to/from default cell readers.
 * 
 * @author Oliver Mannion
 * @version $Revision: 190 $
 */
public class CDataFileDefLoader {

	private final Gson gson;

	/**
	 * Default constructor.
	 */
	public CDataFileDefLoader() {
		GsonBuilder gsonBuilder = new GsonBuilder().setPrettyPrinting();
		gsonBuilder.registerTypeAdapter(CellReader.class,
				new CellReaderJsonSerializer());
		gsonBuilder.registerTypeAdapter(CDataFileDef.class,
				new CDataFileDefJsonCreator());

		gson = gsonBuilder.create();
	}

	/**
	 * Serialize a {@link CDataFileDef} to a JSON string.
	 * 
	 * @param cdef
	 *            casper data file definition
	 * @return a JSON string representing {@code cdef}.
	 * @throws CellReaderException
	 *             if {@code cdef} has any non default cell readers.
	 */
	public String toJsonString(CDataFileDef cdef) {
		return gson.toJson(cdef);
	}

	/**
	 * Construct a {@link CDataFileDef} from a JSON string.
	 * 
	 * @param jsonString
	 *            string containing JSON text of a {@link CDataFileDef}.
	 * @return a {@link CDataFileDef} deserialized from {@code jsonString}.
	 */
	public CDataFileDef fromJsonString(String jsonString) {
		return gson.fromJson(jsonString, CDataFileDef.class);
	}

	/**
	 * Construct a {@link CDataFileDef} from JSON text in a file.
	 * 
	 * @param jsonFile
	 *            file containing JSON text of a {@link CDataFileDef}.
	 * @return a {@link CDataFileDef} deserialized from {@code jsonFile}.
	 * @throws IOException
	 *             if invalid JSON string in {@code jsonFile}, or problem
	 *             reading from {@code jsonFile}.
	 */
	public CDataFileDef fromJsonFile(File jsonFile) throws IOException {
		try {
			return gson.fromJson(new FileReader(jsonFile), CDataFileDef.class);
		} catch (JsonParseException e) {
			throw new IOException("Cannot load " + jsonFile.getCanonicalPath()
					+ ": " + e.getMessage(), e);
		}
	}

	/**
	 * CellReader JSON serializer/deserializer.
	 * 
	 * CellReaders are serialized to a string if they are one of the default
	 * CellReaders from {@link CellReaders}, otherwise a
	 * {@link CellReaderException} is thrown.
	 * 
	 * CellReaders strings are deserialized to a default cell reader.
	 */
	private static class CellReaderJsonSerializer implements
			JsonSerializer<CellReader<?>>, JsonDeserializer<CellReader<?>> {

		@Override
		public JsonElement serialize(CellReader<?> src, Type typeOfSrc,
				JsonSerializationContext context) {
			String crName = CellReaders.getDefaultReaderName(src);
			if (crName == null) {
				throw new CellReaderException("CellReader \"" + src.toString()
						+ "\" is not a default cell reader");
			}
			return new JsonPrimitive(crName);
		}

		@Override
		public CellReader<?> deserialize(JsonElement json, Type typeOfT,
				JsonDeserializationContext context) throws JsonParseException {
			CellReader<?> cr = CellReaders.getDefaultReader(json.getAsString());

			if (cr == null) {
				throw new JsonParseException("No default reader called \""
						+ json.getAsString() + "\"");
			}

			return CellReaders.getDefaultReader(json.getAsString());
		}
	}

	/**
	 * Creates {@link CDataFileDef} with bogus instance values that will get
	 * overwritten during deserialization.
	 */
	private static class CDataFileDefJsonCreator implements
			InstanceCreator<CDataFileDef> {

		@Override
		public CDataFileDef createInstance(Type type) {
			return new CDataFileDef("CDataFileDefJsonCreator", null);
		}

	}
}
