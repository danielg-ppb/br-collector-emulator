package com.br.collector.emulator;

import flutter.gstt.data.betradar_uof.CollectorEnvelopeUofProto;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CollectorService {
    private final CollectorPublisher producer;

    public CollectorService(CollectorPublisher producer) {
        this.producer = producer;
    }

    public void readJsonAndSend(String resourceFileName) throws Exception {
        List<CollectorEnvelopeUofProto.CollectorEnvelope> messages = JsonFileReader.processJsonFileFromResources(resourceFileName);

        for (CollectorEnvelopeUofProto.CollectorEnvelope message : messages) {
            String messageKey = message.getExternalId();
            producer.sendAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey);
        }

    }
}
