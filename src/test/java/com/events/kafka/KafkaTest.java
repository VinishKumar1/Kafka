package com.events.kafka;


import com.events.kafka.service.KafkaReactiveProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.Test;

@SpringBootTest
public class KafkaTest {

    @Autowired
    private KafkaReactiveProducerService producerService;

    @Test
    public void testSendMessage() {
//        var user = com.example.kafka.User.newBuilder().setAge(12)
//                        .setName("John Doe")
//                                .build();
//        producerService.sendMessage("reactive-avro-compressed-topic", user).subscribe();
    }
}

