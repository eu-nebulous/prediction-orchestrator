package eu.nebulouscloud.predictionorchestrator.communication.publishers;


import eu.nebulouscloud.exn.core.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PredictedMetricsPublisher extends Publisher {
    private static final Logger log = LoggerFactory.getLogger(PredictedMetricsPublisher.class);
    String topicName;
    public PredictedMetricsPublisher(String topicName) {
        super("predicted_metrics_" + topicName, "eu.nebulouscloud.monitoring.predicted." + topicName, true, true);
        this.topicName = topicName;
        log.info("Created PredictedMetricsPublisher for topic {}", topicName);
    }
}