package com.br.collector.emulator;

import flutter.gstt.data.betradar_livedata.CollectorEnvelopeLivedataProto;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class CollectorLDService {
    private final CollectorPublisher producer;

    public CollectorLDService(CollectorPublisher producer) {
        this.producer = producer;
    }

    public void readJsonAndSend(String resourceFileName) throws Exception {



        List<CollectorEnvelopeLivedataProto.CollectorEnvelope> messages = JsonFileReader.processLDJsonFileFromResources(resourceFileName);

        for (CollectorEnvelopeLivedataProto.CollectorEnvelope message : messages) {
            String messageKey = message.getExternalId();
            producer.sendLDAdapterOutboundMessage(messageKey, message);
            System.out.println("Message sent with key: " + messageKey);
        }

    }
}
