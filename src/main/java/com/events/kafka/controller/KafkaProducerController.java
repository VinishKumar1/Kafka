package com.events.kafka.controller;

import com.events.kafka.model.User;
import com.events.kafka.service.KafkaReactiveProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class KafkaProducerController {

    private final KafkaReactiveProducerService producerService;

    @PostMapping
    public void sendMessage(@RequestBody User user) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();

            // Repeat the "name" field multiple times for testing compression
            String repeatedName = String.join("", java.util.Collections.nCopies(1000, user.getName()));
            user.setName(repeatedName);

            // Serialize to JSON and send the message
            String message = objectMapper.writeValueAsString(user);
            System.out.println("Serialized message size: " + message.getBytes().length + " bytes");
            producerService.sendStringMessage("my-string-topic_15", message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
