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

    /**
     * Removes a Publisher from the factory, e.g., when connection is lost.
     *
     * @param metricName The name of the metric.
     */
    public void removePublisher(String metricName) {
        publishers.remove(metricName);
    }

    /**
     * Re-register all publishers when the context becomes available again.
     */
    public void reRegisterAllPublishers() {
        Context context = brokerConnectorHandler.getContext();
        if (context == null) {
            log.error("Context is not available. Cannot re-register publishers.");
            return;
        }

        for (String metricName : publishers.keySet()) {
            try {
                Publisher publisher = publishers.get(metricName);
                context.registerPublisher(publisher);
                log.info("Re-registered PredictedMetricsPublisher for metric '{}'.", metricName);
            } catch (Exception e) {
                log.error("Failed to re-register PredictedMetricsPublisher for metric '{}'. Exception: {}", metricName, e.getMessage(), e);
            }
        }
    }

    /**
     * Removes all publishers from the factory, e.g., when connection is lost.
     */
    public void removeAllPublishers() {
        publishers.clear();
    }
}