package org.sagebionetworks.bridge.fitbit.scheduler;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

/** Bridge FitBit Scheduler */
public class BridgeFitBitScheduler {

    // For now, scheduler will assume Seattle time for calculating date. If we want to make timezone configurable, this
    // will be part of the new Smart Scheduler.
    private static final DateTimeZone LOCAL_TIME_ZONE = DateTimeZone.forID("America/Los_Angeles");

    // Visible for testing
    static final String CONFIG_KEY_SCHEDULER_NAME = "schedulerName";
    static final String CONFIG_KEY_QUEUE_URL = "sqsQueueUrl";
    static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
    static final String REQUEST_KEY_BODY = "body";
    static final String REQUEST_KEY_DATE = "date";
    static final String REQUEST_KEY_SERVICE = "service";
    static final String SERVICE = "FitBitWorker";

    private Table ddbFitBitConfigTable;
    private AmazonSQS sqsClient;

    /**
     * DDB table for Scheduler configs. We store configs in DDB instead of in env vars or in a config file because
     * (1) DDB is easier to update for fast config changes and (2) AWS Lambda is a lightweight infrastructure without
     * env vars or config files.
     */
    public final void setDdbFitBitConfigTable(Table ddbFitBitConfigTable) {
        this.ddbFitBitConfigTable = ddbFitBitConfigTable;
    }

    /** SQS client, used to send the request. */
    public final void setSqsClient(AmazonSQS sqsClient) {
        this.sqsClient = sqsClient;
    }

    /**
     * Sends the request for the given scheduler. This is called by AWS Lambda at the configured interval.
     * NOTE: This only sends the request once. The actually scheduling logic is handled by AWS Lambda.
     *
     * @param schedulerName
     *         scheduler name, used to get scheduler configs
     * @throws IOException
     *         if constructing the request fails
     */
    public void schedule(String schedulerName) throws IOException {
        // Get scheduler config from DDB
        Item schedulerConfig = ddbFitBitConfigTable.getItem(CONFIG_KEY_SCHEDULER_NAME, schedulerName);
        Preconditions.checkNotNull(schedulerConfig, "No configuration for scheduler " + schedulerName);

        String sqsQueueUrl = schedulerConfig.getString(CONFIG_KEY_QUEUE_URL);
        Preconditions.checkArgument(!Strings.isNullOrEmpty(sqsQueueUrl), "sqsQueueUrl not configured for scheduler "
                + schedulerName);

        // Request has only parameter: yesterday's date
        LocalDate yesterdaysDate = LocalDate.now(LOCAL_TIME_ZONE).minusDays(1);

        // Make request JSON
        ObjectNode bodyNode = JSON_OBJECT_MAPPER.createObjectNode();
        bodyNode.put(REQUEST_KEY_DATE, yesterdaysDate.toString());

        ObjectNode requestNode = JSON_OBJECT_MAPPER.createObjectNode();
        requestNode.put(REQUEST_KEY_SERVICE, SERVICE);
        requestNode.set(REQUEST_KEY_BODY, bodyNode);

        // write request to SQS
        String requestJson = JSON_OBJECT_MAPPER.writeValueAsString(requestNode);
        System.out.println("Sending request: sqsQueueUrl=" + sqsQueueUrl + ", requestJson=" + requestJson);
        sqsClient.sendMessage(sqsQueueUrl, requestJson);
    }
}
