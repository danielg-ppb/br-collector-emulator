package com.br.collector.emulator;

import flutter.gstt.data.betradar_uof.CollectorEnvelopeUofProto;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CollectorUofService {
    private final CollectorPublisher producer;

    public CollectorUofService(CollectorPublisher producer) {
        this.producer = producer;
    }

    public void readJsonAndSend(String resourceFileName) throws Exception {
        List<CollectorEnvelopeUofProto.CollectorEnvelope> messages = JsonFileReader.processUoFJsonFileFromResources(resourceFileName);

        for (CollectorEnvelopeUofProto.CollectorEnvelope message : messages) {
            String messageKey = message.getExternalId();
            producer.sendUofAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey);
        }

    }
}
