package com.example.springboot;


import com.eventstore.dbclient.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import com.example.springboot.entity.*;


@RestController
@RequestMapping("/events/integrations")
public class IntegrationsEventStoreController {

//    private final String STREAM_NAME = "integrations";

    EventStoreDBClient client;

    ObjectMapper mapper;

    public IntegrationsEventStoreController() {
        this.mapper = new ObjectMapper();
        EventStoreDBClientSettings settings = EventStoreDBConnectionString.parseOrThrow("esdb+discover://localhost:2113?keepAliveTimeout=10000&keepAliveInterval=10000&Tls=false");
        this.client = EventStoreDBClient.create(settings);
        System.out.println("DONE!!");
    }

    @PostMapping("/insert")
    public String insert(@RequestBody String body) throws ExecutionException, InterruptedException, JsonProcessingException {

        final var reqBodyJson = mapper.readTree(body);

        IntegrationEvent eventObj = IntegrationEvent.CreateIntegrationEvent(reqBodyJson);

        EventData event = EventData
                .builderAsJson("integration_event", eventObj)
                .build();

        var streamName = String.valueOf(eventObj.getSourceId());
        WriteResult writeResult = client.appendToStream(streamName, event).get();

        return writeResult.toString();

    }

    @GetMapping("/get")
    public ResponseEntity get(@RequestParam long sourceid, @RequestParam(required = false, defaultValue = "10") int numEvents) throws ExecutionException, InterruptedException, IOException {
        ReadStreamOptions readStreamOptions = ReadStreamOptions.get()
                .fromStart()
                .maxCount(numEvents)
                .notResolveLinkTos();

        var streamName = String.valueOf(sourceid);
        ReadResult readResult = client.readStream(streamName, readStreamOptions).get();
        List<ResolvedEvent> resolvedEvents = readResult.getEvents();

        var ints2 = new ArrayList<JsonNode>();
        for(var e: resolvedEvents) {
            ints2.add(mapper.readTree(e.getOriginalEvent().getEventData()));
        }


        return new ResponseEntity<>(ints2, HttpStatus.OK);
//        return mapper.writeValueAsString(ints2);
    }

}
