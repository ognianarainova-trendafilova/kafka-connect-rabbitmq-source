package com.ibm.eventstreams.connect.rabbitmqsource.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public class ValueSchema {

    private static final String FIELD_MESSAGE_CONSUMERTAG = "consumerTag";
    private static final String FIELD_MESSAGE_ENVELOPE = "envelope";
    private static final String FIELD_MESSAGE_BASICPROPERTIES = "basicProperties";
    private static final String FIELD_MESSAGE_BODY = "body";

    private Struct struct;
    private final Schema schema;

    public ValueSchema(boolean keepBytesBody) {
        SchemaBuilder bodySchema;
        if (keepBytesBody) {
            bodySchema = SchemaBuilder.bytes();
        } else {
            bodySchema = SchemaBuilder.string();
        }

        schema = SchemaBuilder.struct()
                .name("MESSAGE: ")
                .doc("Message as it is delivered to the RabbitMQ Consumer. ")
                .field(FIELD_MESSAGE_CONSUMERTAG, SchemaBuilder.string().doc("The consumer tag associated with the consumer").build())
                .field(FIELD_MESSAGE_ENVELOPE, EnvelopeSchema.SCHEMA)
                .field(FIELD_MESSAGE_BASICPROPERTIES, BasicPropertiesSchema.SCHEMA)
                .field(FIELD_MESSAGE_BODY, bodySchema.build())
                .build();
    }

    public synchronized Struct getStruct(String consumerTag, Envelope envelope, AMQP.BasicProperties basicProperties, byte[] body) {
        if (struct != null) {
            struct = new Struct(schema)
                    .put(FIELD_MESSAGE_CONSUMERTAG, consumerTag)
                    .put(FIELD_MESSAGE_ENVELOPE, EnvelopeSchema.toStruct(envelope))
                    .put(FIELD_MESSAGE_BASICPROPERTIES, BasicPropertiesSchema.toStruct(basicProperties))
                    .put(FIELD_MESSAGE_BODY, body);
        }

        return struct;
    }

    public Object getMessageBody() {
        if (struct == null) {
            return null;
        }

        if (getBodySchema() == Schema.BYTES_SCHEMA) {
            return struct.getBytes(FIELD_MESSAGE_BODY);
        }

        return struct.getString(FIELD_MESSAGE_BODY);
    }

    public Schema getBodySchema() {
        if (schema == null) {
            return Schema.STRING_SCHEMA;
        }

        return schema.field(FIELD_MESSAGE_BODY).schema();
    }
}
