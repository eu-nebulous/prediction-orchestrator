package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.predictionorchestrator.consumers.MetricsListConsumer;
import eu.nebulouscloud.predictionorchestrator.messages.Metric;
import eu.nebulouscloud.predictionorchestrator.messages.MetricListMessage;
import eu.nebulouscloud.predictionorchestrator.messages.StartForecastingMessage;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import java.util.*;

import static com.github.javaparser.utils.Utils.assertNotNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MetricsListConsumerTest {


    private MetricsListConsumer.MetricsListHandler handler;

    @BeforeEach
    public void setUp() {
        handler = new MetricsListConsumer.MetricsListHandler();
    }

    @Test
    public void testMapToStartForecastingMessage() {
        // Given
        Map<String, Object> body = new HashMap<>();
        body.put("name", "_Application1");
        body.put("version", 1);

        List<Map<String, String>> metricsMap = Arrays.asList(
                createMetricMap("metric_1", "100.0", "0.0"),
                createMetricMap("metric_2", "Infinity", "-Infinity"),
                createMetricMap("metric_3", "10.0", "-4.0")
        );

        body.put("metric_list", metricsMap);

        MetricListMessage metricListMessage = new MetricListMessage(
                (String) body.get("name"),
                (Integer) body.get("version"),
                Arrays.asList(
                        new Metric("metric_1", "100.0", "0.0"),
                        new Metric("metric_2", "Infinity", "-Infinity"),
                        new Metric("metric_3", "10.0", "-4.0")
                )
        );

        // Expected values
        long timestamp = 1705046535L;
        long epochStart = 1705046500L;
        int numberOfForwardPredictions = 5;
        int predictionHorizon = 120;

        // When
        StartForecastingMessage startForecastingMessage = MetricsListConsumer.MetricsListHandler.mapToStartForecastingMessage(
                metricListMessage, timestamp, epochStart, numberOfForwardPredictions, predictionHorizon
        );

        List<String> metricList = new ArrayList<>(List.of("metric_1", "metric_2", "metric_3"));

        // Then
        assertNotNull(startForecastingMessage);
        assertEquals("_Application1", startForecastingMessage.getName());
        assertArrayEquals(metricList.toArray(), startForecastingMessage.getMetrics().toArray());
        assertEquals(timestamp, startForecastingMessage.getTimestamp());
        assertEquals(epochStart, startForecastingMessage.getEpochStart());
        assertEquals(numberOfForwardPredictions, startForecastingMessage.getNumberOfForwardPredictions());
        assertEquals(predictionHorizon, startForecastingMessage.getPredictionHorizon());

    }

    private Map<String, String> createMetricMap(String name, String upperBound, String lowerBound) {
        Map<String, String> map = new HashMap<>();
        map.put("name", name);
        map.put("upper_bound", upperBound);
        map.put("lower_bound", lowerBound);
        return map;
    }
}
