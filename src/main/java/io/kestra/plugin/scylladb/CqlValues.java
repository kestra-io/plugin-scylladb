package io.kestra.plugin.scylladb;

import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Reversible storage encoding for query results and prepared acknowledgement binds.
 * Non-string map keys become {@code {"key","value"}} lists so key types survive Ion round-trips;
 * decoding always uses the prepared marker's CQL type.
 */
final class CqlValues {
    private CqlValues() {
    }

    static Map<String, Object> row(Row row) {
        Map<String, Object> values = new LinkedHashMap<>();
        var columns = row.getColumnDefinitions();
        for (int i = 0; i < columns.size(); i++) {
            values.put(columns.get(i).getName().asInternal(), toStorage(row.getObject(i)));
        }
        return values;
    }

    static Object toStorage(Object value) {
        if (value == null || value instanceof String || value instanceof Boolean || value instanceof Number) {
            return value;
        }
        if (value instanceof UUID || value instanceof Instant || value instanceof LocalDate
            || value instanceof LocalTime || value instanceof CqlDuration) {
            return value.toString();
        }
        if (value instanceof InetAddress address) {
            return address.getHostAddress();
        }
        if (value instanceof ByteBuffer buffer) {
            ByteBuffer remaining = buffer.duplicate();
            byte[] bytes = new byte[remaining.remaining()];
            remaining.get(bytes);
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof UdtValue udt) {
            Map<String, Object> fields = new LinkedHashMap<>();
            var names = udt.getType().getFieldNames();
            for (int i = 0; i < names.size(); i++) {
                fields.put(names.get(i).asInternal(), toStorage(udt.getObject(i)));
            }
            return fields;
        }
        if (value instanceof TupleValue tuple) {
            List<Object> fields = new ArrayList<>(tuple.size());
            for (int i = 0; i < tuple.size(); i++) {
                fields.add(toStorage(tuple.getObject(i)));
            }
            return fields;
        }
        if (value instanceof Map<?, ?> map) {
            if (map.keySet().stream().allMatch(String.class::isInstance)) {
                Map<String, Object> values = new LinkedHashMap<>();
                map.forEach((key, item) -> values.put((String) key, toStorage(item)));
                return values;
            }
            List<Map<String, Object>> entries = new ArrayList<>(map.size());
            map.forEach((key, item) -> {
                Map<String, Object> entry = new LinkedHashMap<>();
                entry.put("key", toStorage(key));
                entry.put("value", toStorage(item));
                entries.add(entry);
            });
            return entries;
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> items = new ArrayList<>();
            iterable.forEach(item -> items.add(toStorage(item)));
            return items;
        }
        throw new IllegalArgumentException("Unsupported CQL result value type: " + value.getClass().getName());
    }

    /**
     * Restores a storage value to the Java type accepted by the marker's default codec.
     * Bind the returned value with an explicit codec, e.g.
     * {@code builder.set(index, fromStorage(value, type, codecs), codecs.<Object>codecFor(type))}.
     */
    static Object fromStorage(Object value, DataType type, CodecRegistry codecs) {
        if (value == null) {
            return null;
        }
        if (type instanceof ListType listType) {
            List<Object> items = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                items.add(fromStorage(item, listType.getElementType(), codecs));
            }
            return items;
        }
        if (type instanceof SetType setType) {
            var items = new LinkedHashSet<>();
            for (Object item : (Iterable<?>) value) {
                items.add(fromStorage(item, setType.getElementType(), codecs));
            }
            return items;
        }
        if (type instanceof MapType mapType) {
            Map<Object, Object> items = new LinkedHashMap<>();
            if (value instanceof Map<?, ?> map) {
                map.forEach((key, item) -> items.put(
                    fromStorage(key, mapType.getKeyType(), codecs),
                    fromStorage(item, mapType.getValueType(), codecs)
                ));
            } else {
                for (Object item : (Iterable<?>) value) {
                    Map<?, ?> entry = (Map<?, ?>) item;
                    items.put(
                        fromStorage(entry.get("key"), mapType.getKeyType(), codecs),
                        fromStorage(entry.get("value"), mapType.getValueType(), codecs)
                    );
                }
            }
            return items;
        }
        if (type instanceof TupleType tupleType) {
            List<?> fields = (List<?>) value;
            if (fields.size() != tupleType.getComponentTypes().size()) {
                throw new IllegalArgumentException("Stored tuple does not match the CQL tuple arity");
            }
            TupleValue tuple = tupleType.newValue();
            for (int i = 0; i < fields.size(); i++) {
                DataType fieldType = tupleType.getComponentTypes().get(i);
                tuple = tuple.set(i, fromStorage(fields.get(i), fieldType, codecs), codecs.<Object>codecFor(fieldType));
            }
            return tuple;
        }
        if (type instanceof UserDefinedType udtType) {
            Map<?, ?> fields = (Map<?, ?>) value;
            UdtValue udt = udtType.newValue();
            for (int i = 0; i < udtType.getFieldNames().size(); i++) {
                DataType fieldType = udtType.getFieldTypes().get(i);
                Object field = fields.get(udtType.getFieldNames().get(i).asInternal());
                udt = udt.set(i, fromStorage(field, fieldType, codecs), codecs.<Object>codecFor(fieldType));
            }
            return udt;
        }

        TypeCodec<Object> codec = codecs.codecFor(type);
        if (codec.getJavaType().getRawType().isInstance(value)) {
            return value;
        }
        String text = value.toString();
        if (type.equals(DataTypes.UUID) || type.equals(DataTypes.TIMEUUID)) {
            return UUID.fromString(text);
        }
        if (type.equals(DataTypes.TIMESTAMP)) {
            return Instant.parse(text);
        }
        if (type.equals(DataTypes.DATE)) {
            return LocalDate.parse(text);
        }
        if (type.equals(DataTypes.TIME)) {
            return LocalTime.parse(text);
        }
        if (type.equals(DataTypes.DURATION)) {
            return CqlDuration.from(text);
        }
        if (type.equals(DataTypes.BLOB)) {
            return ByteBuffer.wrap(value instanceof byte[] bytes ? bytes : Base64.getDecoder().decode(text));
        }
        if (type.equals(DataTypes.INET)) {
            try {
                return InetAddress.getByName(text);
            } catch (UnknownHostException e) {
                throw new IllegalArgumentException("Invalid stored CQL inet value", e);
            }
        }
        // Ion/JSON can widen small integers and floats. Restore the exact marker type
        // instead of relying on prepared-statement bind(Object...) inference.
        if (type.equals(DataTypes.TINYINT)) {
            return new BigDecimal(text).byteValueExact();
        }
        if (type.equals(DataTypes.SMALLINT)) {
            return new BigDecimal(text).shortValueExact();
        }
        if (type.equals(DataTypes.INT)) {
            return new BigDecimal(text).intValueExact();
        }
        if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER)) {
            return new BigDecimal(text).longValueExact();
        }
        if (type.equals(DataTypes.VARINT)) {
            return new BigDecimal(text).toBigIntegerExact();
        }
        if (type.equals(DataTypes.DECIMAL)) {
            return new BigDecimal(text);
        }
        if (type.equals(DataTypes.FLOAT)) {
            return Float.valueOf(text);
        }
        if (type.equals(DataTypes.DOUBLE)) {
            return Double.valueOf(text);
        }
        throw new IllegalArgumentException("Stored value does not match CQL type " + type);
    }
}
