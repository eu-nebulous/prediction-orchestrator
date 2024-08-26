package eu.nebulouscloud.predictionorchestrator.consumers;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.publishers.PredictedMetricsPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.util.Map;

@Slf4j
public class IntermediateMetricConsumer extends Consumer {

    public IntermediateMetricConsumer(String applicationName) {
        super("intermediate_metrics_consumer_" + applicationName,
                "eu.nebulouscloud.monitoring.preliminary_predicted.>",
                new IntermediateMetricsHandler(applicationName),
                true, true);
    }

    public static class IntermediateMetricsHandler extends Handler {
        private String applicationName;

        public IntermediateMetricsHandler(String applicationName) {
            this.applicationName = applicationName;
        }

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            log.info("Received message with key: {}, address: {}", key, address);
            log.info("Intermediate Metric body: {}", body);

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
                log.info("New PredictedMetricsPublisher for metric {}", metricName);
            }
            predictedMetricsPublisher.send(body, applicationName);
            log.info("Sent predicted metric to topic: {}", body);
        }
    }
}
