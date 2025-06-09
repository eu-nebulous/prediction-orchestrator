package eu.nebulouscloud.predictionorchestrator.communication;

import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Publisher;
import eu.nebulouscloud.predictionorchestrator.communication.connectors.BrokerConnectorHandler;
import eu.nebulouscloud.predictionorchestrator.communication.publishers.PredictedMetricsPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
@Slf4j
public class PublisherFactory {

    @Autowired
    private BrokerConnectorHandler brokerConnectorHandler;

    // Thread-safe map to store Publishers by metric name
    private ConcurrentHashMap<String, Publisher> publishers = new ConcurrentHashMap<>();

    /**
     * Retrieves an existing Publisher for the given metric or creates and registers a new one.
     *
     * @param metricName The name of the metric.
     * @return The Publisher instance, or null if registration fails.
     */
    public Publisher getOrCreatePublisher(String metricName) {
        Context context = brokerConnectorHandler.getContext();
        if (context == null) {
            log.error("Context is not available. Cannot create or retrieve Publisher for metric '{}'.", metricName);
            return null;
        }

        // Check if Publisher already exists
        Publisher publisher = publishers.get(metricName);
        if (publisher != null) {
            return publisher;
        }

        synchronized (this) {
            // Double-check within synchronized block
            publisher = publishers.get(metricName);
            if (publisher == null) {
                try {
                    log.info("Creating and registering new PredictedMetricsPublisher for metric '{}'.", metricName);
                    publisher = new PredictedMetricsPublisher(metricName);
                    context.registerPublisher(publisher);
                    publishers.put(metricName, publisher);
                    log.info("PredictedMetricsPublisher for metric '{}' registered successfully.", metricName);
                } catch (Exception e) {
                    log.error("Failed to create or register PredictedMetricsPublisher for metric '{}'. Exception: {}", metricName, e.getMessage(), e);
                    return null;
                }
            }
        }
        return publisher;
    }

}