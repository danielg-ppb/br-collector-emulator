package com.br.collector.emulator;

import flutter.gstt.data.betradar_livedata.CollectorEnvelopeLivedataProto;
import flutter.gstt.data.betradar_uof.CollectorEnvelopeUofProto;
import org.apache.pulsar.client.api.MessageId;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class BetRadarCollectorService {
    private final BetRadarCollectorPublisher producer;

    public BetRadarCollectorService(BetRadarCollectorPublisher producer) {
        this.producer = producer;
    }

    public void readUofJsonAndSend(String resourceFileName) throws Exception {
        List<CollectorEnvelopeUofProto.CollectorEnvelope> messages = JsonFileReader.processUoFJsonFileFromResources(resourceFileName);

        messages.forEach(message -> {
            String messageKey = message.getExternalId();
            MessageId messageId = producer.sendUofAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey + " and messageId: " + messageId);
        });

    }

    public void readLdJsonAndSend(String resourceFileName) throws Exception {

        List<CollectorEnvelopeLivedataProto.CollectorEnvelope> messages = JsonFileReader.processLDJsonFileFromResources(resourceFileName);

        messages.forEach(message -> {
            String messageKey = message.getExternalId();
            MessageId messageId = producer.sendLDAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey + " and messageId: " + messageId);
        });

    }
}
