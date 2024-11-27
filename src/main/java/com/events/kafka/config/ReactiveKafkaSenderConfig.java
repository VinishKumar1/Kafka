package com.events.kafka.config;

import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;
import reactor.kafka.sender.SenderRecord;

@Configuration
public class ReactiveKafkaSenderConfig {

    @Bean
    public KafkaSender<String, String> kafkaSender() {
        Map<String, Object> props = new HashMap<>();

        // Kafka Broker configuration
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "com.events.kafka.config.KafkaMessageSizeInterceptor");

        // Enable compression (e.g., LZ4, GZIP)
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // Optional: Optimize for large message handling
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 20971520); // 20 MB
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536);         // 16 KB batch size
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);              // Reduce latency for small batches

        SenderOptions<String, String> senderOptions = SenderOptions.create(props);
        return KafkaSender.create(senderOptions);
    }

    // Monitor data size before sending
    public long calculateMessageSize(String message) {
        return message.getBytes(StandardCharsets.UTF_8).length;
    }

    public void sendCompressedMessage(String topic, String message) {
        long originalSize = calculateMessageSize(message);
        System.out.println("Original message size: " + originalSize + " bytes");

        kafkaSender().send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, message), null)))
                .doOnNext(result -> System.out.println("Message sent successfully"))
                .doOnError(error -> System.err.println("Error sending message: " + error.getMessage()))
                .subscribe();
    }
}
