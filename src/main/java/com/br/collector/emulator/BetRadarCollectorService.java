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

    public List<CollectorEnvelopeUofProto.CollectorEnvelope> readUofJson(String resourceFileName) throws Exception {
        return JsonFileReader.processUoFJsonFileFromResources(resourceFileName);
    }

    public void sendUofMessages(List<CollectorEnvelopeUofProto.CollectorEnvelope> messages) {
        messages.forEach(message -> {
            String messageKey = message.getExternalId();
            MessageId messageId = producer.sendUofAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey + " and messageId: " + messageId);
        });
    }

    public void sendLdMessages(List<CollectorEnvelopeLivedataProto.CollectorEnvelope> messages) {
        messages.forEach(message -> {
            String messageKey = message.getExternalId();
            MessageId messageId = producer.sendLDAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey + " and messageId: " + messageId);
        });
    }

    public List<CollectorEnvelopeLivedataProto.CollectorEnvelope> readLdJson(String resourceFileName) throws Exception {
        return JsonFileReader.processLDJsonFileFromResources(resourceFileName);
    }


}
