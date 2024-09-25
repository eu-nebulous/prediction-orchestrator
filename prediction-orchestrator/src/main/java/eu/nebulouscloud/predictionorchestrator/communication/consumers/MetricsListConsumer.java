package eu.nebulouscloud.predictionorchestrator.communication.consumers;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.predictionorchestrator.config.MethodConfig;
import eu.nebulouscloud.predictionorchestrator.Orchestrator;
import eu.nebulouscloud.predictionorchestrator.Properties;
import eu.nebulouscloud.predictionorchestrator.communication.messages.Metric;
import eu.nebulouscloud.predictionorchestrator.communication.messages.MetricListMessage;
import eu.nebulouscloud.predictionorchestrator.communication.messages.StartForecastingMessage;
import eu.nebulouscloud.predictionorchestrator.communication.publishers.StartForecastingPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.exn.core.Context;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

@Slf4j
@Component
public class MetricsListConsumer extends Consumer {

    private final Properties properties;

    @Autowired
    public MetricsListConsumer(Orchestrator orchestrator, Properties properties) {
        super("metric_list_consumer",
                "eu.nebulouscloud.monitoring.metric_list",
                new MetricsListHandler(orchestrator, properties),
                true, true);
        this.properties = properties;
    }

    public static class MetricsListHandler extends Handler {

        private static final ConcurrentMap<String, Long> applicationVersions = new ConcurrentHashMap<>();
        private final Orchestrator orchestrator;
        private final Properties properties;

        public MetricsListHandler(Orchestrator orchestrator, Properties properties) {
            this.orchestrator = orchestrator;
            this.properties = properties;
        }

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            try {
                String appName = (String) body.get("name");
                long version = (Long) body.get("version");
                log.info("Metric list received for application {} with version {}", appName, version);

                // Check if the version has changed
                Long lastVersion = applicationVersions.get(appName);
                if (lastVersion != null && lastVersion.equals(version)) {
                    log.info("Version {} for application {} has not changed. Ignoring message.", version, appName);
                    return;
                }

                // Update the application's version in the registry
                applicationVersions.put(appName, version);

                // Retrieve and process metrics list from the message body
                List<Map<String, String>> metricsMap = (List<Map<String, String>>) body.get("metric_list");
                List<Metric> metrics = metricsMap.stream()
                        .map(m -> new Metric(m.get("name"), m.get("upper_bound"), m.get("lower_bound")))
                        .collect(Collectors.toList());

                List<String> metricNames = metrics.stream()
                        .map(Metric::getName)
                        .collect(Collectors.toList());

                // Create a new MetricListMessage for processing
                MetricListMessage metricListMessage = new MetricListMessage(appName, version, metrics);

                long epochStart = System.currentTimeMillis() / 1000;  // Ensure consistency by using seconds

                if (Objects.equals(metricListMessage.getName(), "_Application1")) {
                    // Register the application with the orchestrator
                    orchestrator.addApplication(appName,
                            LocalDateTime.ofEpochSecond(epochStart, 0, ZoneOffset.UTC),
                            metricNames
                    );
                }

                // Prepare StartForecastingMessage with consistent timestamping
                StartForecastingMessage startForecastingMessage = mapToStartForecastingMessage(
                        metricListMessage,
                        epochStart,    // Use the same `epochStart` as above
                        epochStart,    // Use `epochStart` for consistency
                        8,             // Example: 8 forward predictions
                        properties.getInitial_prediction_horizon()            // Example: 120 seconds horizon
                );

                // Get the list of forecasting methods
                List<String> forecastingMethods = MethodConfig.getMethodNames();

                // Process each forecasting method
                for (String method : forecastingMethods) {
                    try {
                        String publisherKey = "start_forecasting_" + method + "_" + appName;

                        StartForecastingPublisher startForecastingPublisher;

                        // Synchronize the access to publisher creation
                        synchronized (this) {
                            startForecastingPublisher = (StartForecastingPublisher) ctx.getPublisher(publisherKey);

                            if (startForecastingPublisher == null) {
                                log.info("StartForecastingPublisher for method {} not found, creating a new one.", method);
                                startForecastingPublisher = new StartForecastingPublisher(method, appName);
                                ctx.registerPublisher(startForecastingPublisher);
                                log.info("New StartForecastingPublisher for method {} registered successfully.", method);
                            } else {
                                log.warn("Publisher {} is already registered.", publisherKey);
                            }
                        }

                        // Double-check if the publisher is null after synchronization
                        if (startForecastingPublisher == null) {
                            log.error("Publisher is null for method {}. Could not send message.", method);
                            continue;  // Skip to the next method if the publisher is null
                        }

                        // Safely send the message using the publisher
                        try {
                            if (Objects.equals(metricListMessage.getName(), "_Application1")) {
                                startForecastingPublisher.send(startForecastingMessageToMap(startForecastingMessage), metricListMessage.getName());
                                log.info("Start forecasting event published for application {}; method {}; message: {}", appName, method, startForecastingMessageToMap(startForecastingMessage));
                            }
                        } catch (NullPointerException e) {
                            log.error("Publisher is null for method {}. Could not send message. Details: {}", method, e.getMessage(), e);
                        } catch (Exception e) {
                            log.error("Unexpected error while sending the message for method {}: {}", method, e.getMessage(), e);
                        }

                    } catch (Exception e) {
                        log.error("Error while processing forecasting method {}: {}", method, e.getMessage(), e);
                    }
                }
            } catch (Exception e) {
                log.error("Error in onMessage: {}", e.getMessage(), e);
            }
        }

        public static StartForecastingMessage mapToStartForecastingMessage(MetricListMessage metricListMessage,
                                                                           long timestamp, long epochStart,
                                                                           int numberOfForwardPredictions, int predictionHorizon) {
            List<String> metricNames = metricListMessage.getMetricList().stream()
                    .map(Metric::getName)
                    .collect(Collectors.toList());

            StartForecastingMessage startForecastingMessage = new StartForecastingMessage();
            startForecastingMessage.setName(metricListMessage.getName());
            startForecastingMessage.setMetrics(metricNames);
            startForecastingMessage.setTimestamp(timestamp);
            startForecastingMessage.setEpochStart(epochStart);
            startForecastingMessage.setNumberOfForwardPredictions(numberOfForwardPredictions);
            startForecastingMessage.setPredictionHorizon(predictionHorizon);

            return startForecastingMessage;
        }

        private static Map<String, Object> startForecastingMessageToMap(StartForecastingMessage message) {
            Map<String, Object> map = new HashMap<>();
            map.put("name", message.getName());
            map.put("metrics", message.getMetrics());
            map.put("timestamp", message.getTimestamp());
            map.put("epoch_start", message.getEpochStart());
            map.put("number_of_forward_predictions", message.getNumberOfForwardPredictions());
            map.put("prediction_horizon", message.getPredictionHorizon());
            return map;
        }
    }
}