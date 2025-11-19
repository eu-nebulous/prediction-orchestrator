package eu.nebulouscloud.predictionorchestrator.communication.consumers;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.communication.publishers.PredictedMetricsPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RealtimeApplicationMetricConsumer extends Consumer {

    public RealtimeApplicationMetricConsumer(String applicationName) {
        super("realtime_metrics_consumer_" + applicationName,
                "eu.nebulouscloud.monitoring.realtime.>",
                new ApplicationMetricsHandler(applicationName),
                true, true);
    }

    public static class ApplicationMetricsHandler extends Handler {
        private String applicationName;

        public ApplicationMetricsHandler(String applicationName) {
            this.applicationName = applicationName;
        }

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
        	try {
            log.debug("Received message with key: {}, address: {}", key, address);

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

            // Remove "app_wide_scope_" prefix if present
            if (metricName.startsWith("app_wide_scope_")) {
                metricName = metricName.substring(16);
                log.debug("Removed 'app_wide_scope_' prefix. New metric name: {}", metricName);
            }

            String publisherKey = "predicted_metrics_" + metricName;
            PredictedMetricsPublisher predictedMetricsPublisher = (PredictedMetricsPublisher) ctx.getPublisher(publisherKey);

            if (predictedMetricsPublisher == null) {
                log.info("PredictedMetricsPublisher for metric {} not found, creating a new one.", metricName);
                predictedMetricsPublisher = new PredictedMetricsPublisher(metricName);
                ctx.registerPublisher(predictedMetricsPublisher);
                log.info("New PredictedMetricsPublisher for metric {} registered with topic: {}", metricName, predictedMetricsPublisher.topic);
            }
            predictedMetricsPublisher.send(predictedMetric, applicationName);
            log.info("Sent predicted metric to topic: {}", predictedMetric);
        	}catch(Exception ex)
        	{
        		log.error("Failed processing key:{}, address:{}, body:{}",key,address,body,ex );
        	}
        }

        private Map<String, Object> transformToPredictedMetric(Map<String, Object> metric) {
            Map<String, Object> predictedMetric = new HashMap<>(metric);
            Double metricValue = (Double) metric.get("metricValue");
            double level = (double) metric.get("level");


            predictedMetric.put("timestamp", System.currentTimeMillis() / 1000);
            // Set the prediction probability to 0.60 as a naive prediction
            predictedMetric.put("probability", 0.60);
            predictedMetric.put("level", level);
            predictedMetric.put("metricValue", metricValue);
            predictedMetric.put("confidence_interval", Arrays.asList(metricValue, metricValue));
            // Use the current system time as the prediction time
            predictedMetric.put("predictionTime", System.currentTimeMillis() / 1000);

            return predictedMetric;
        }
    }
}