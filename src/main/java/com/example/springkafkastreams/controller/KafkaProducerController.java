package com.example.springkafkastreams.controller;

import com.example.springkafkastreams.service.KafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducerController {

    @Autowired
    private KafkaProducer kafkaProducer;

    @PostMapping("/publish")
    public ResponseEntity<String> publishMessage(@RequestParam String message) {
        kafkaProducer.sendMessage(message);
        return ResponseEntity.ok(message);
    }
}

