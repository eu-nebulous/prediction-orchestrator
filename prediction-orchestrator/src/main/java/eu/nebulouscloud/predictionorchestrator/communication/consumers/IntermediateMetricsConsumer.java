package eu.nebulouscloud.predictionorchestrator.communication.consumers;


import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.Prediction;
import eu.nebulouscloud.predictionorchestrator.PredictionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;

import java.util.List;
import java.util.Map;

@Slf4j
public class IntermediateMetricsConsumer extends Consumer {

    private final PredictionRegistry predictionRegistry;

    public IntermediateMetricsConsumer(String applicationName, PredictionRegistry predictionRegistry) {
        super("intermediate_metrics_consumer_" + applicationName,
                "eu.nebulouscloud.monitoring.preliminary_predicted.>",
                new IntermediateMetricsHandler(applicationName, predictionRegistry),
                true, true);
        this.predictionRegistry = predictionRegistry;
    }

    public static class IntermediateMetricsHandler extends Handler {
        private final String applicationName;
        private final PredictionRegistry predictionRegistry;

        public IntermediateMetricsHandler(String applicationName, PredictionRegistry predictionRegistry) {
            this.applicationName = applicationName;
            this.predictionRegistry = predictionRegistry;
        }

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            log.info("Received message with key: {}, address: {}", key, address);
            log.info("Intermediate Metric body: {}", body);

            String[] parts;
            try {
                parts = message.to().split("\\.");
                String predictionMethodName = parts[3];
                String metricName = parts[4];

                Prediction prediction = new Prediction(
                        Double.parseDouble(body.get("metricValue").toString()),
                        (String) body.get("componentId"),
                        Long.parseLong(body.get("timestamp").toString()),
                        Double.parseDouble(body.get("probability").toString()),
                        (List<Double>) body.get("confidenceInterval"),
                        Long.parseLong(body.get("predictionTime").toString()),
                        predictionMethodName,
                        metricName
                );

                predictionRegistry.storePrediction(applicationName, prediction);

            } catch (Exception e) {
                log.error("Failed to process message", e);
                throw new RuntimeException("Failed to process message", e);
            }
        }
    }
}