package com.br.collector.emulator;

import com.google.protobuf.util.JsonFormat;
import flutter.gstt.data.betradar_uof.CollectorEnvelopeUofProto;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.pulsar.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class JsonFileReader {
    public static List<CollectorEnvelopeUofProto.CollectorEnvelope> processJsonFileFromResources(String resourceFileName) throws Exception {
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
                entities.add(builder.build());
            }
        }
        return entities;
    }
}
