package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.predictionorchestrator.influx.InfluxDBService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.apache.commons.collections4.queue.CircularFifoQueue;


import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class PredictionRegistry {
    private final InfluxDBService influxDBService;
    private final Properties properties = new Properties();

    // New data structure that includes timestamp organization
    private final Map<String, Map<String, Map<String, Map<Long, CircularFifoQueue<Prediction>>>>> predictionQueues = new ConcurrentHashMap<>();

    private int queueSize;

    public PredictionRegistry(InfluxDBService influxDBService) {
        this.influxDBService = influxDBService;

        int defaultQueueSize = 20;
        this.queueSize = Math.max(properties.getInitial_forward_prediction_number(), defaultQueueSize);
    }

    // Get or create a queue organized by timestamp
    public CircularFifoQueue<Prediction> getOrCreateQueue(String appName, String metricName, String method, long timestamp) {
        return predictionQueues
                .computeIfAbsent(appName, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(metricName, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(method, k -> new ConcurrentHashMap<>())
                .computeIfAbsent(timestamp, k -> new CircularFifoQueue<>(queueSize));
    }

    // Store a prediction by method and timestamp
    public void storePrediction(String appName, String metricName, String method, Prediction prediction) {
        CircularFifoQueue<Prediction> queue = getOrCreateQueue(appName, metricName, method, prediction.getPredictionTime());
        queue.add(prediction); // Automatically discards the oldest prediction when full
        storePredictionInfluxDb(appName,prediction, method);
    }

    // Get the latest prediction for each method at the specified timestamp
    public Map<String, Prediction> getPredictionsByMethodAndTimestamp(String appName, String metricName, long timestamp) {
        Map<String, Map<Long, CircularFifoQueue<Prediction>>> methodQueues = predictionQueues
                .computeIfAbsent(appName, k -> new ConcurrentHashMap<>())
                .getOrDefault(metricName, new ConcurrentHashMap<>());

        // Collect the most recent prediction for each method at the given timestamp
        Map<String, Prediction> predictionsByMethod = new HashMap<>();
        for (Map.Entry<String, Map<Long, CircularFifoQueue<Prediction>>> entry : methodQueues.entrySet()) {
            CircularFifoQueue<Prediction> queue = entry.getValue().get(timestamp);
            if (queue != null && !queue.isEmpty()) {
                predictionsByMethod.put(entry.getKey(), queue.peek());
            }
        }
        return predictionsByMethod;
    }

    // Cleanup old predictions for a specific timestamp
    public void cleanupOldPredictions(String appName, String metricName, long timestamp) {
        Map<String, Map<Long, CircularFifoQueue<Prediction>>> methodQueues = predictionQueues
                .computeIfAbsent(appName, k -> new ConcurrentHashMap<>())
                .getOrDefault(metricName, new ConcurrentHashMap<>());

        for (Map<Long, CircularFifoQueue<Prediction>> timestampQueues : methodQueues.values()) {
            timestampQueues.remove(timestamp);  // Remove only the queue for the specified timestamp
        }

//        log.debug("Cleaned up old predictions for application {} and metric {} at timestamp {}", appName, metricName, timestamp);
    }

    // Store the ensembled prediction in InfluxDB
    public void storePredictionInfluxDb(String applicationName, Prediction ensembledPrediction, String method) {
        try {
            // Write the ensembled prediction to InfluxDB under the specified bucket
            influxDBService.writePrediction(applicationName, ensembledPrediction, method);
        } catch (Exception e) {
            log.error("Failed to store ensembled prediction for application {}", applicationName, e);
        }
    }
    public void printAllPredictionsForApplication(String targetAppName) {
        Map<String, Map<String, Map<Long, CircularFifoQueue<Prediction>>>> metricsMap
                = predictionQueues.get(targetAppName);

        if (metricsMap == null) {
            log.info("No predictions found for application '{}'.", targetAppName);
            return;
        }

        log.info("Predictions for application '{}':", targetAppName);

        // Iterate over metrics
        for (Map.Entry<String, Map<String, Map<Long, CircularFifoQueue<Prediction>>>> metricEntry
                : metricsMap.entrySet()) {
            String metricName = metricEntry.getKey();
            Map<String, Map<Long, CircularFifoQueue<Prediction>>> methodsMap = metricEntry.getValue();

            // Iterate over methods
            for (Map.Entry<String, Map<Long, CircularFifoQueue<Prediction>>> methodEntry
                    : methodsMap.entrySet()) {
                String methodName = methodEntry.getKey();
                Map<Long, CircularFifoQueue<Prediction>> timestampsMap = methodEntry.getValue();

                // Iterate over timestamps
                for (Map.Entry<Long, CircularFifoQueue<Prediction>> timestampEntry
                        : timestampsMap.entrySet()) {
                    Long timestamp = timestampEntry.getKey();
                    CircularFifoQueue<Prediction> queue = timestampEntry.getValue();

                    // Print each prediction
                    for (Prediction prediction : queue) {
                        log.info("  Metric: {}, Method: {}, Timestamp: {}, Prediction: {}",
                                metricName, methodName, timestamp, prediction);
                    }
                }
            }
        }
    }

}