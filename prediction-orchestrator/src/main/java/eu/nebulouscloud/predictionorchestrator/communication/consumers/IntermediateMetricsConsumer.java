package eu.nebulouscloud.predictionorchestrator.communication.consumers;


import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.Prediction;
import eu.nebulouscloud.predictionorchestrator.PredictionRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Slf4j
public class IntermediateMetricsConsumer extends Consumer {

    public IntermediateMetricsConsumer(PredictionRegistry predictionRegistry) {
        super("intermediate_metrics_consumer",
                "eu.nebulouscloud.monitoring.preliminary_predicted.>",
                new IntermediateMetricsHandler(predictionRegistry),
                true, true);
    }

    public static class IntermediateMetricsHandler extends Handler {
        private final PredictionRegistry predictionRegistry;

        // Constructor for handler that takes applicationName and predictionRegistry
        public IntermediateMetricsHandler(PredictionRegistry predictionRegistry) {
            this.predictionRegistry = predictionRegistry;  // Store the passed registry
        }

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            log.info("Received message with key: {}, address: {}", key, address);
            log.info("Intermediate Metric body: {}", body);

            String[] parts;
            try {
                parts = message.to().split("\\.");
                String predictionMethodName = parts[4];
                String metricName = parts[5];

                Prediction prediction = new Prediction(
                        Double.parseDouble(body.get("metricValue").toString()),
                        (String) body.get("componentId"),
                        Long.parseLong(body.get("timestamp").toString()),
                        Double.parseDouble(body.get("probability").toString()),
                        (List<Double>) body.get("confidence_interval"),
                        Long.parseLong(body.get("predictionTime").toString()),
                        predictionMethodName,
                        metricName
                );
                String applicationName = message.subject();
                predictionRegistry.storePrediction(applicationName, metricName, predictionMethodName, prediction);

            } catch (Exception e) {
            	log.error("Failed processing key:{}, address:{}, body:{}",key,address,body,e);
            }
        }
    }
}