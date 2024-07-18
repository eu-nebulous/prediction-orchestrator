package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.predictionorchestrator.consumers.RealtimeApplicationMetricConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

@Slf4j
public class ApplicationSpecificPredictionConsumerTest {

    private RealtimeApplicationMetricConsumer.ApplicationMetricsHandler handler;
    private Context mockContext;
    private PredictedMetricsPublisher mockPublisher;
    private Message mockMessage;

    @BeforeEach
    public void setUp() {
        String applicationName = "testApp";
        handler = new RealtimeApplicationMetricConsumer.ApplicationMetricsHandler(applicationName);
        mockContext = mock(Context.class);
        mockPublisher = mock(PredictedMetricsPublisher.class);
        mockMessage = mock(Message.class);
    }

    @Test
    public void testOnMessage() throws ClientException {
        String key = "someKey";
        String address = "topic://eu.nebulouscloud.monitoring.realtime.RawProcessingLatency";
        Map<String, Object> body = new HashMap<>();
        body.put("metricValue", 10.401474237442017);
        body.put("level", 1);
        body.put("timestamp", 1719394731);

        when(mockMessage.to()).thenReturn("topic://eu.nebulouscloud.monitoring.realtime.RawProcessingLatency");
        when(mockContext.getPublisher(anyString())).thenReturn(mockPublisher);

        handler.onMessage(key, address, body, mockMessage, mockContext);

        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        verify(mockPublisher, times(1)).send(captor.capture(), eq("testApp"));

        Map<String, Object> capturedMessage = captor.getValue();

        assertEquals(10.401474237442017, capturedMessage.get("metricValue"));
        assertEquals(1, capturedMessage.get("level"));
        assertEquals(0.60, capturedMessage.get("probability"));
        // Add more assertions as needed to verify the content of the message
    }
}