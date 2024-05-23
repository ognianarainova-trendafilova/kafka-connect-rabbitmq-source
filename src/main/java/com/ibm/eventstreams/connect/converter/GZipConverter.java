package com.ibm.eventstreams.connect.converter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

/**
 * {@link Converter} implementation that only supports compressing and
 * decompressing GZip. When converting Kafka Connect data to bytes, the schema
 * will be ignored and {@link Object#toString()} with UTF-8 encoding will always
 * be invoked to convert the data to a String. When converting from bytes to
 * Kafka Connect format, the converter will only ever return an optional string
 * schema and a string or null. This implementation currently does nothing with
 * the topic name.
 */
public class GZipConverter implements Converter {

    public GZipConverter() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        if (value == null) {
            return null;
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream(); GZIPOutputStream ds = new GZIPOutputStream(out)) {
            ds.write(value.toString().getBytes(StandardCharsets.UTF_8));
            ds.close();

            return out.toByteArray();
        } catch (IOException e) {
            throw new DataException("Failed to decompress gzip: ", e);
        }
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        if (value == null) {
            return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, value);
        }

        try (InputStream is = new ByteArrayInputStream(value); InputStream ds = new GZIPInputStream(is); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = ds.read(buffer)) > -1) {
                out.write(buffer, 0, len);
            }
            out.flush();

            return new SchemaAndValue(Schema.OPTIONAL_STRING_SCHEMA, out.toByteArray());
        } catch (IOException e) {
            throw new DataException("Failed to compress gzip: ", e);
        }
    }
}
