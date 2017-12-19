package org.sagebionetworks.bridge.fitbit.scheduler;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.sqs.AmazonSQS;
import com.fasterxml.jackson.databind.JsonNode;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BridgeFitBitSchedulerTest {
    private static final String QUEUE_URL = "dummy-queue-url";
    private static final String SCHEDULER_NAME = "test-scheduler";

    // Mock NOW is 2017-12-18T7:00Z. In local (Seattle) time, this is 2017-12-17T23:00-0800. This means that
    // yesterday's date is 2017-12-16.
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2017-12-18T7:00Z").getMillis();

    private Table mockConfigTable;
    private AmazonSQS mockSqsClient;
    private BridgeFitBitScheduler scheduler;

    @BeforeClass
    public static void mockNow() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @AfterClass
    public static void cleanup() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @BeforeMethod
    public void setup() {
        mockConfigTable = mock(Table.class);
        mockSqsClient = mock(AmazonSQS.class);

        scheduler = new BridgeFitBitScheduler();
        scheduler.setDdbFitBitConfigTable(mockConfigTable);
        scheduler.setSqsClient(mockSqsClient);
    }

    @Test
    public void normalCase() throws Exception {
        // Mock DDB config
        Item configItem = new Item().withString(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)
                .withString(BridgeFitBitScheduler.CONFIG_KEY_QUEUE_URL, QUEUE_URL);
        when(mockConfigTable.getItem(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)).thenReturn(
                configItem);

        // Execute
        scheduler.schedule(SCHEDULER_NAME);

        // Verify SQS request
        ArgumentCaptor<String> requestTextCaptor = ArgumentCaptor.forClass(String.class);
        verify(mockSqsClient).sendMessage(eq(QUEUE_URL), requestTextCaptor.capture());

        JsonNode requestNode = BridgeFitBitScheduler.JSON_OBJECT_MAPPER.readTree(requestTextCaptor.getValue());
        assertEquals(requestNode.size(), 2);
        assertEquals(requestNode.get(BridgeFitBitScheduler.REQUEST_KEY_SERVICE).textValue(),
                BridgeFitBitScheduler.SERVICE);

        JsonNode bodyNode = requestNode.get(BridgeFitBitScheduler.REQUEST_KEY_BODY);
        assertEquals(bodyNode.size(), 1);
        assertEquals(bodyNode.get(BridgeFitBitScheduler.REQUEST_KEY_DATE).textValue(), "2017-12-16");
    }

    // branch coverage
    @Test(expectedExceptions = NullPointerException.class)
    public void nullConfig() throws Exception {
        when(mockConfigTable.getItem(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)).thenReturn(
                null);
        scheduler.schedule(SCHEDULER_NAME);
    }

    // branch coverage
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void nullQueue() throws Exception {
        Item configItem = new Item().withString(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)
                .withNull(BridgeFitBitScheduler.CONFIG_KEY_QUEUE_URL);
        when(mockConfigTable.getItem(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)).thenReturn(
                configItem);
        scheduler.schedule(SCHEDULER_NAME);
    }

    // branch coverage
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void emptyQueue() throws Exception {
        Item configItem = new Item().withString(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)
                .withString(BridgeFitBitScheduler.CONFIG_KEY_QUEUE_URL, "");
        when(mockConfigTable.getItem(BridgeFitBitScheduler.CONFIG_KEY_SCHEDULER_NAME, SCHEDULER_NAME)).thenReturn(
                configItem);
        scheduler.schedule(SCHEDULER_NAME);
    }
}
