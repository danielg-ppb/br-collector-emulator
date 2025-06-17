package com.br.collector.emulator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CollectorEmulator implements CommandLineRunner {
    @Autowired
    private CollectorService collectorService;

    public static void main(String[] args) {
        SpringApplication.run(CollectorEmulator.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        collectorService.readJsonAndSend("fixtureChange.json");
        collectorService.readJsonAndSend("oddChange.json");
        System.out.println("Finished sending messages to Pulsar.");
    }

}
