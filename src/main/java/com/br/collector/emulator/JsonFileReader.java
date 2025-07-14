package com.br.collector.emulator;

import com.flutter.gstt.proto.Metadata;
import com.google.protobuf.util.JsonFormat;
import flutter.gstt.data.betradar_livedata.CollectorEnvelopeLivedataProto;
import flutter.gstt.data.betradar_uof.CollectorEnvelopeUofProto;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class JsonFileReader {

    public static final String BETRADAR_PROVIDER = "BetRadar";

    public static List<CollectorEnvelopeUofProto.CollectorEnvelope> processUoFJsonFileFromResources(String resourceFileName) throws Exception {
        ClassPathResource resource = new ClassPathResource(resourceFileName);
        List<CollectorEnvelopeUofProto.CollectorEnvelope> entities = new ArrayList<>();

        try (InputStream inputStream = resource.getInputStream()) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(inputStream);

            if (!rootNode.isArray()) {
                throw new IllegalArgumentException("Expected a JSON array");
            }

            for (JsonNode node : rootNode) {
                CollectorEnvelopeUofProto.CollectorEnvelope.Builder builder = CollectorEnvelopeUofProto.CollectorEnvelope.newBuilder();
                JsonFormat.parser().merge(node.toString(), builder);


                Metadata updatedMetadata = builder.getMetadata().toBuilder()
                        .setProvider(BETRADAR_PROVIDER)
                        .setCorrelationId(builder.getMetadata().getCorrelationId() + "-emulator")
                        .build();

                builder.setMetadata(updatedMetadata);

                entities.add(builder.build());
            }
        }
        return entities;
    }


    public static List<CollectorEnvelopeLivedataProto.CollectorEnvelope> processLDJsonFileFromResources(String resourceFileName) throws Exception {
        ClassPathResource resource = new ClassPathResource(resourceFileName);
        List<CollectorEnvelopeLivedataProto.CollectorEnvelope> entities = new ArrayList<>();

        try (InputStream inputStream = resource.getInputStream()) {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(inputStream);

            if (!rootNode.isArray()) {
                throw new IllegalArgumentException("Expected a JSON array");
            }

            for (JsonNode node : rootNode) {
                CollectorEnvelopeLivedataProto.CollectorEnvelope.Builder builder = CollectorEnvelopeLivedataProto.CollectorEnvelope.newBuilder();
                JsonFormat.parser().merge(node.toString(), builder);

                Metadata updatedMetadata = builder.getMetadata().toBuilder()
                        .setProvider(BETRADAR_PROVIDER)
                        .setCorrelationId(builder.getMetadata().getCorrelationId() + "-emulator")
                        .build();

                builder.setMetadata(updatedMetadata);
                entities.add(builder.build());
            }
        }
        return entities;
    }
}
