package com.events.kafka.service;

import java.nio.charset.StandardCharsets;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

@Service
public class KafkaReactiveConsumerService {

//    @KafkaListener(topics = "reactive-avro-topic_8", groupId = "reactive-consumer-group")
//    public Mono<Void> consume(ConsumerRecord<String, Object> record) {
//        return Mono.fromRunnable(() -> {
//            try {
//                // Extract the payload from the ConsumerRecord
//                Object message = record.value();
//
//                // If the message is a GenericData.Record, calculate its size
//                if (message instanceof GenericData.Record) {
//                    long sizeAfterDecompression = getSizeAfterDecompression((GenericData.Record) message);
////                    System.out.println("Received message: " + message);
//                    System.out.println("Size after decompression: " + sizeAfterDecompression);
//                } else {
//                    System.out.println("Received non-Avro message: " + message);
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        });
//    }

    // Utility method to calculate the size of an Avro object (GenericData.Record)
    private long getSizeAfterDecompression(GenericData.Record record) throws IOException {
        // Use Avro's encoder to write the record into a byte array
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericData.Record> datumWriter = new org.apache.avro.specific.SpecificDatumWriter<>(record.getSchema());
        Encoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);

        datumWriter.write(record, encoder);  // Write the record to the encoder
        encoder.flush();

        return byteArrayOutputStream.size()/1024;  // Return the size of the serialized Avro record
    }



    @KafkaListener(topics = "my-string-topic_15", groupId = "my-string-group")
    public void listen(String message) {
            long size = message.getBytes(StandardCharsets.UTF_8).length;
            System.out.println("Received message of size: " + size + " bytes");
//        System.out.println("Received message: " + message);
    }

}
