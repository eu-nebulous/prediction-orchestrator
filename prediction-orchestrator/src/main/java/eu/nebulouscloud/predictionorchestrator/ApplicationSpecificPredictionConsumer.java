package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ApplicationSpecificPredictionConsumer extends Consumer {

    public ApplicationSpecificPredictionConsumer(String applicationName) {
        super("realtime_metrics_consumer_" + applicationName,
                "eu.nebulouscloud.monitoring.realtime.>",
                new ApplicationMetricsHandler(applicationName),
                true, true);
    }

    private static class ApplicationMetricsHandler extends Handler {
        private String applicationName;

        public ApplicationMetricsHandler(String applicationName) {
            this.applicationName = applicationName;
        }

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            log.debug("Received message with key: {}, address: {}", key, address); // Added more context to the log message

            // Transform Type I message to Type II format (predicted metrics)
            Map<String, Object> predictedMetric = transformToPredictedMetric(body);
            log.info("Transformed message body to predicted metrics: {}", predictedMetric);

            String[] parts;
            try {
                parts = message.to().split("\\.");
            } catch (ClientException e) {
                log.error("Failed to split message 'to' property", e);
                throw new RuntimeException("Failed to split message 'to' property", e);
            }

            // Assuming the metric name is the last part of the topic
            String metricName = parts[parts.length - 1];
            log.debug("Extracted metric name: {}", metricName); // Log the extracted metric name

            String publisherKey = "predicted_metrics_" + metricName;
            PredictedMetricsPublisher predictedMetricsPublisher = (PredictedMetricsPublisher) ctx.getPublisher(publisherKey);

            if (predictedMetricsPublisher == null) {
                log.info("PredictedMetricsPublisher for metric {} not found, creating a new one.", metricName);
                predictedMetricsPublisher = new PredictedMetricsPublisher(metricName);
                ctx.registerPublisher(predictedMetricsPublisher);
                log.info("New PredictedMetricsPublisher for metric {} registered.", metricName);
            }

            predictedMetricsPublisher.send(predictedMetric, applicationName);
            log.info("Sent predicted metric to topic: {}", predictedMetricsPublisher.topicName);
        }

        private Map<String, Object> transformToPredictedMetric(Map<String, Object> metric) {
            Map<String, Object> predictedMetric = new HashMap<>(metric);

            // Set the prediction confidence to 0.60 as a naive prediction
            predictedMetric.put("prediction_confidence", 0.60);

            // Set the prediction interval with a length of zero
            Double metricValue = (Double) metric.get("metricValue");
            predictedMetric.put("prediction_interval", Arrays.asList(metricValue, metricValue));

            // Use the current system time as the prediction time
            predictedMetric.put("prediction_time", System.currentTimeMillis() / 1000);

            return predictedMetric;
        }
    }
}