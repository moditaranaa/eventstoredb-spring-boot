package com.example.springboot.entity;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



@Getter
@Setter
public class IntegrationEvent {

    // timestamp
    long eventTime;

    // event type
    IntegrationEventType eventType;

    // source id on which the event happened
    long sourceId;

    // userid of the user who performed the action
    long actionUserId;

    // any message that you want to save with this event. ex - "reloading this integration due to xyz issue"
    String eventMessage;

    JsonNode newSourceSettings;
    JsonNode oldSourceSettings;

    public static IntegrationEvent CreateIntegrationEvent(JsonNode reqBodyJson) throws JsonProcessingException {

        var mapper = new ObjectMapper();
        IntegrationEvent eventObj = new IntegrationEvent();

        eventObj.setSourceId(reqBodyJson.get("sourceId").asLong());
        eventObj.setEventType(IntegrationEventType.valueOf(reqBodyJson.get("eventType").asText().toUpperCase()));
        eventObj.setActionUserId(reqBodyJson.get("userId").asLong());

        var eventTime = System.currentTimeMillis();
        if(reqBodyJson.get("eventTime") != null && reqBodyJson.get("eventTime").asLong() > 0) {
            eventTime = reqBodyJson.get("eventTime").asLong();
        }
        eventObj.setEventTime(eventTime);

        var eventMessage = "";
        if(reqBodyJson.get("eventMessage") != null) {
            eventMessage = reqBodyJson.get("eventMessage").asText();
        }
        eventObj.setEventMessage(eventMessage);

        var newSourceSettings = reqBodyJson.get("newSourceSettings") == null ? "" : reqBodyJson.get("newSourceSettings").toString();
        eventObj.setNewSourceSettings(mapper.readTree(newSourceSettings));

        var oldSourceSettings = reqBodyJson.get("oldSourceSettings") == null ? "" : reqBodyJson.get("oldSourceSettings").toString();
        eventObj.setOldSourceSettings(mapper.readTree(oldSourceSettings));

        return eventObj;
    }

};


class SourceSettings {
    SourceToken token;
    List<String> accounts = new ArrayList<>();
    Map<String, String> props;
    int maxDepthOfNesting;

    UpdateMode updateMode = UpdateMode.INSERT;
    int versionInt = 1;
    JsonNode config = null;
    String columnHashingSal = "";
    long batchSize = -1;
    boolean enableDatonMetaDataTableForBigQuery = false;

};


class SourceToken{
    String token = "";
    Long expires = 0L;
    String machineId = null;
    String mailId = null;
};



enum  UpdateMode {
    INHERIT, INSERT, UPSERT, WRITE_TRUNCATE
};