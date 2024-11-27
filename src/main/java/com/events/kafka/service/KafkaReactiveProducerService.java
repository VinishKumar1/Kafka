package com.events.kafka.service;

import com.events.kafka.config.ReactiveKafkaSenderConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
public class KafkaReactiveProducerService {

//  private final KafkaSender<String, Object> kafkaSender;
//
//  public KafkaReactiveProducerService(KafkaSender<String, Object> kafkaSender) {
//    this.kafkaSender = kafkaSender;
//  }

  //    public Mono<Void> sendMessage(String topic, com.example.kafka.User message) {
  //        try{
  //        long sizeBeforeCompression = new
  // ReactiveKafkaSenderConfig().getSizeBeforeCompression(message);
  //        System.out.println("Size before compression: " + sizeBeforeCompression);
  //        return kafkaSender.send(Mono.just(SenderRecord.create(topic, null, null, null, message,
  // null)))
  //                .doOnNext(result -> System.out.println("Message sent to topic: " + topic))
  //                .doOnError(error -> System.err.println("Failed to send message: " +
  // error.getMessage()))
  //                .then();
  //        } catch (Exception e) {
  //            return Mono.error(e);
  //        }
  //    }


    @Autowired
    private KafkaSender<String, String> kafkaSender;

    public void sendStringMessage(String topic, String message) {
        kafkaSender.send(Mono.just(SenderRecord.create(new ProducerRecord<>(topic, message), null)))
                .subscribe(result -> System.out.println("Message sent successfully: "),
                        error -> System.err.println("Error sending message: " + error.getMessage()));
    }
}
