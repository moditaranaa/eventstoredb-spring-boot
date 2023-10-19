package com.example.springboot;


import com.eventstore.dbclient.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;


class IntegrationCreated {

    private UUID id;
    private String name;

    public IntegrationCreated(String name) {
        this.name = name;
        this.id = UUID.randomUUID();
    }

    public UUID getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }
}

@RestController
@RequestMapping("/events")
public class EventStoreController {

    private final String STREAM_NAME = "integrations";

    EventStoreDBClient client;

    ObjectMapper mapper;

    public EventStoreController() {
        this.mapper = new ObjectMapper();
        EventStoreDBClientSettings settings = EventStoreDBConnectionString.parseOrThrow("esdb+discover://localhost:2113?keepAliveTimeout=10000&keepAliveInterval=10000&Tls=false");
        this.client = EventStoreDBClient.create(settings);
        System.out.println("DONE!!");
    }

    @GetMapping("/insert")
    public String insert() throws ExecutionException, InterruptedException {

        int x = new Random().nextInt();
        String name = "googleads" + Integer.toString(x);

        IntegrationCreated createdEvent = new IntegrationCreated(name);

        EventData event = EventData
                .builderAsJson("IntegrationCreated", createdEvent)
                .build();

        WriteResult writeResult = client.appendToStream(STREAM_NAME, event).get();

        return writeResult.toString();

    }

    @GetMapping("/get")
    public String get() throws ExecutionException, InterruptedException, IOException {

        ReadStreamOptions readStreamOptions = ReadStreamOptions.get()
                .fromStart()
                .maxCount(10)
                .notResolveLinkTos();

        ReadResult readResult = client.readStream(STREAM_NAME,readStreamOptions).get();
        List<ResolvedEvent> resolvedEvents = readResult.getEvents();

        var ints2 = new ArrayList<JsonNode>();
        for(var e: resolvedEvents) {
            ints2.add(mapper.readTree(e.getOriginalEvent().getEventData()));
        }

        return mapper.writeValueAsString(ints2);
    }

}
