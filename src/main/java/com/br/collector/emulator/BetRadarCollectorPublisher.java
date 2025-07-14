package com.br.collector.emulator;

import flutter.gstt.data.betradar_livedata.CollectorEnvelopeLivedataProto;
import flutter.gstt.data.betradar_uof.CollectorEnvelopeUofProto;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.springframework.pulsar.PulsarException;
import org.springframework.pulsar.core.PulsarTemplate;
import org.springframework.stereotype.Component;

import static org.apache.pulsar.client.api.BatcherBuilder.KEY_BASED;

@Component
public class BetRadarCollectorPublisher {

    public static final String UOF_TOPIC = "persistent://gst/tennis-feed/betradar-uof-messages";
    public static final String LD_TOPIC = "persistent://gst/tennis-feed/betradar-ld-messages";

    private final PulsarTemplate<CollectorEnvelopeUofProto.CollectorEnvelope> uofProducer;
    private final PulsarTemplate<CollectorEnvelopeLivedataProto.CollectorEnvelope> ldProducer;


    public BetRadarCollectorPublisher(PulsarTemplate<CollectorEnvelopeUofProto.CollectorEnvelope> producer, PulsarTemplate<CollectorEnvelopeLivedataProto.CollectorEnvelope> ldProducer) {
        this.uofProducer = producer;
        this.ldProducer = ldProducer;
    }

    public MessageId sendUofAdapterOutboundMessage(
            String messageKey,
            CollectorEnvelopeUofProto.CollectorEnvelope entityEnvelope) {
        try {
            return uofProducer.newMessage(entityEnvelope)
                    .withTopic(UOF_TOPIC)
                    .withProducerCustomizer(producerBuilder -> producerBuilder.batcherBuilder(KEY_BASED))
                    .withMessageCustomizer(messageBuilder -> messageBuilder.key(messageKey))
                    .withSchema(Schema.PROTOBUF(CollectorEnvelopeUofProto.CollectorEnvelope.class))
                    .send();


        } catch (Exception e) {
            System.out.println("Message failed to be sent to Pulsar topic:" + LD_TOPIC + ". Message ID: " + messageKey + ". Error: " + e.getMessage());
            throw new PulsarException("Failed to send message to pulsar. Message ID: " + messageKey, e);
        }
    }

    public MessageId sendLDAdapterOutboundMessage(
            String messageKey,
            CollectorEnvelopeLivedataProto.CollectorEnvelope entityEnvelope) {
        try {

            return ldProducer.newMessage(entityEnvelope)
                    .withTopic(LD_TOPIC)
                    .withProducerCustomizer(producerBuilder -> producerBuilder.batcherBuilder(KEY_BASED))
                    .withMessageCustomizer(messageBuilder -> messageBuilder.key(messageKey))
                    .withSchema(Schema.PROTOBUF(CollectorEnvelopeLivedataProto.CollectorEnvelope.class))
                    .send();

        } catch (Exception e) {

            System.out.println("Message failed to be sent to Pulsar topic:" + LD_TOPIC + ". Message ID: " + messageKey + ". Error: " + e.getMessage());
            throw new PulsarException("Failed to send message to pulsar. Message ID: " + messageKey, e);
        }
    }


}
