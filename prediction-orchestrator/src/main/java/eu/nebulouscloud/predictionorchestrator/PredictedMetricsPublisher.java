package eu.nebulouscloud.predictionorchestrator;

import eu.nebulouscloud.exn.core.Publisher;

class PredictedMetricsPublisher extends Publisher {
    String topicName;
    public PredictedMetricsPublisher(String topicName) {
        super("predicted_metrics_" + topicName, "eu.nebulouscloud.monitoring.predicted." + topicName, true, true);
        this.topicName = topicName;
    }
}