package io.kestra.plugin.scylladb;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import io.kestra.core.serializers.FileSerde;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CqlValuesTest {
    private static final CodecRegistry CODECS = CodecRegistry.DEFAULT;

    @Test
    void roundTripsScalarTypesThroughIon() throws Exception {
        assertRoundTrip(UUID.fromString("ae196b1f-c830-422f-b364-daf0f3b203fe"), DataTypes.UUID);
        assertRoundTrip(Instant.parse("2026-10-03T04:05:06.123Z"), DataTypes.TIMESTAMP);
        assertRoundTrip(LocalDate.of(2026, 10, 3), DataTypes.DATE);
        assertRoundTrip(LocalTime.of(4, 5, 6, 123456789), DataTypes.TIME);
        assertRoundTrip(InetAddress.getByName("127.0.0.1"), DataTypes.INET);
        assertRoundTrip(CqlDuration.newInstance(2, 3, 123456789), DataTypes.DURATION);
        assertRoundTrip(new BigDecimal("1234567890.123456789"), DataTypes.DECIMAL);
        assertRoundTrip(new BigInteger("123456789012345678901234567890"), DataTypes.VARINT);
        assertRoundTrip((byte) 12, DataTypes.TINYINT);
        assertRoundTrip((short) 1234, DataTypes.SMALLINT);
        assertRoundTrip(123456, DataTypes.INT);
        assertRoundTrip(12345678901L, DataTypes.BIGINT);
        assertRoundTrip(1.25F, DataTypes.FLOAT);
        assertRoundTrip(1.25D, DataTypes.DOUBLE);
        assertRoundTrip(true, DataTypes.BOOLEAN);
    }

    @Test
    void copiesOnlyRemainingBytesWithoutChangingReadOnlyBuffer() throws Exception {
        ByteBuffer original = ByteBuffer.allocateDirect(5);
        original.put(new byte[] { 0, 1, 2, 3, 4 }).flip();
        original.position(1).limit(4);
        ByteBuffer readOnly = original.asReadOnlyBuffer();

        Object stored = CqlValues.toStorage(readOnly);
        assertEquals("AQID", stored);
        assertEquals(1, readOnly.position());
        assertEquals(4, readOnly.limit());
        assertEquals(ByteBuffer.wrap(new byte[] { 1, 2, 3 }), restore(stored, DataTypes.BLOB));
        assertRoundTrip(readOnly, DataTypes.BLOB);
    }

    @Test
    void roundTripsNestedCollectionsAndTypedMapKeys() throws Exception {
        UUID key = UUID.fromString("ae196b1f-c830-422f-b364-daf0f3b203fe");
        var type = DataTypes.mapOf(DataTypes.UUID, DataTypes.listOf(DataTypes.setOf(DataTypes.DATE)));
        Map<UUID, List<Set<LocalDate>>> original = Map.of(key, List.of(Set.of(LocalDate.of(2026, 10, 3))));

        Object stored = CqlValues.toStorage(original);
        assertInstanceOf(List.class, stored);
        assertEquals(key.toString(), ((Map<?, ?>) ((List<?>) stored).getFirst()).get("key"));
        assertRoundTrip(original, type);
    }

    @Test
    void preservesStringKeyMapsAndNullFields() throws Exception {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("first", null);
        original.put("second", UUID.fromString("ae196b1f-c830-422f-b364-daf0f3b203fe"));
        Map<?, ?> stored = (Map<?, ?>) CqlValues.toStorage(original);

        assertEquals(Arrays.asList("first", "second"), new ArrayList<>(stored.keySet()));
        assertTrue(stored.containsKey("first"));
        assertNull(stored.get("first"));
        assertEquals(original.get("second").toString(), stored.get("second"));
        assertNull(restore(null, DataTypes.INT));
    }

    @Test
    void roundTripsTupleWithNullComponent() throws Exception {
        var type = DataTypes.tupleOf(DataTypes.UUID, DataTypes.INT, DataTypes.BLOB);
        TupleValue tuple = type.newValue()
            .setUuid(0, UUID.fromString("ae196b1f-c830-422f-b364-daf0f3b203fe"))
            .setToNull(1)
            .setByteBuffer(2, ByteBuffer.wrap(new byte[] { 1, 2, 3 }));

        assertRoundTrip(tuple, type);
        assertThrows(IllegalArgumentException.class, () -> restore(List.of("one"), type));
    }

    @Test
    void roundTripsComplexMapKeysWithoutStringification() throws Exception {
        var keyType = DataTypes.tupleOf(DataTypes.INT, DataTypes.TEXT);
        TupleValue key = keyType.newValue().setInt(0, 42).setString(1, "key");
        assertRoundTrip(Map.of(key, "value"), DataTypes.mapOf(keyType, DataTypes.TEXT));
    }

    @Test
    void roundTripsUdtWithQuotedNamesAndNestedTuple() throws Exception {
        var tupleType = DataTypes.tupleOf(DataTypes.TIMESTAMP, DataTypes.SMALLINT);
        var type = new DefaultUserDefinedType(
            CqlIdentifier.fromInternal("test"),
            CqlIdentifier.fromInternal("event"),
            true,
            List.of(CqlIdentifier.fromInternal("MixedCase"), CqlIdentifier.fromInternal("optional")),
            List.of(tupleType, DataTypes.TEXT)
        );
        var tuple = tupleType.newValue()
            .setInstant(0, Instant.parse("2026-10-03T04:05:06.123Z"))
            .setShort(1, (short) 42);
        var udt = type.newValue().setTupleValue(0, tuple).setToNull(1);
        Map<?, ?> stored = (Map<?, ?>) CqlValues.toStorage(udt);
        assertTrue(stored.containsKey("MixedCase"));
        assertTrue(stored.containsKey("optional"));
        assertNull(stored.get("optional"));
        assertRoundTrip(udt, type);
    }

    @Test
    void rejectsOutOfRangeMarkerValuesAndUnsupportedObjects() {
        assertThrows(ArithmeticException.class, () -> restore(128, DataTypes.TINYINT));
        assertThrows(ArithmeticException.class, () -> restore(new BigDecimal("1.5"), DataTypes.INT));
        assertThrows(IllegalArgumentException.class, () -> CqlValues.toStorage(new Object()));
    }

    @Test
    void validatesRenderedStatementAndPageSize() {
        assertThrows(IllegalArgumentException.class, () -> QueryService.validate(null, null));
        assertThrows(IllegalArgumentException.class, () -> QueryService.validate("  ", null));
        assertThrows(IllegalArgumentException.class, () -> QueryService.validate("SELECT * FROM system.local", 0));
        assertThrows(IllegalArgumentException.class, () -> QueryService.validate("SELECT * FROM system.local", -1));
        assertDoesNotThrow(() -> QueryService.validate("SELECT * FROM system.local", null));
        assertDoesNotThrow(() -> QueryService.validate("SELECT * FROM system.local", 1));
    }

    private static Object restore(Object value, DataType type) {
        return CqlValues.fromStorage(value, type, CODECS);
    }

    private static void assertRoundTrip(Object original, DataType type) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FileSerde.write(output, Map.of("value", CqlValues.toStorage(original)));
        List<Object> rows = new ArrayList<>();
        try (var reader = new BufferedReader(new StringReader(output.toString(StandardCharsets.UTF_8)))) {
            FileSerde.reader(reader, rows::add);
        }
        assertEquals(1, rows.size());
        Object restored = restore(((Map<?, ?>) rows.getFirst()).get("value"), type);
        assertEquals(original, restored);
        assertTrue(CODECS.codecFor(type).getJavaType().getRawType().isInstance(restored));
    }
}
