package eu.nebulouscloud.predictionorchestrator.consumers;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
import eu.nebulouscloud.predictionorchestrator.MethodConfig;
import eu.nebulouscloud.predictionorchestrator.messages.Metric;
import eu.nebulouscloud.predictionorchestrator.messages.MetricListMessage;
import eu.nebulouscloud.predictionorchestrator.messages.StartForecastingMessage;
import eu.nebulouscloud.predictionorchestrator.publishers.PredictedMetricsPublisher;
import eu.nebulouscloud.predictionorchestrator.publishers.StartForecastingPublisher;
import lombok.extern.slf4j.Slf4j;
import org.apache.qpid.protonj2.client.Message;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
@Slf4j
public class MetricsListConsumer extends Consumer {
    public MetricsListConsumer() {
        super("metric_list_consumer",
                "eu.nebulouscloud.monitoring.metric_list",
                new MetricsListHandler(),
                true, true);
    }

    public static class MetricsListHandler extends Handler {
        private static final ConcurrentMap<String, Long> applicationVersions = new ConcurrentHashMap<>();

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            try {
                // Extracting data from the body map
                String name = (String) body.get("name");
                long version = (Long) body.get("version");
                log.info("Metric list received for application {} with version {}", name, version);

                // Check if the version has changed
                Long lastVersion = applicationVersions.get(name);
                if (lastVersion != null && lastVersion == version) {
                    log.info("Version {} for application {} has not changed. Ignoring message.", version, name);
                    return;
                }
                applicationVersions.put(name, version);

                List<Map<String, String>> metricsMap = (List<Map<String, String>>) body.get("metric_list");

                // Converting maps to Metric objects
                List<Metric> metrics = metricsMap.stream()
                        .map(m -> new Metric(m.get("name"), m.get("upper_bound"), m.get("lower_bound")))
                        .collect(Collectors.toList());

                // Creating MetricListMessage
                MetricListMessage metricListMessage = new MetricListMessage(name, version, metrics);

                // Example values for the additional parameters
                long timestamp = System.currentTimeMillis() / 1000;
                long epochStart = System.currentTimeMillis();
                int numberOfForwardPredictions = 5;
                int predictionHorizon = 120;

                // Mapping to StartForecastingMessage
                StartForecastingMessage startForecastingMessage = mapToStartForecastingMessage(metricListMessage,
                        timestamp, epochStart,
                        numberOfForwardPredictions, predictionHorizon);

                // Processing each metric for different forecasting methods
                List<String> forecastingMethods = MethodConfig.getMethodNames();
                for (String method : forecastingMethods) {
                    try {
                        String publisherKey = "start_forecasting_" + method + "_" + name;

                        StartForecastingPublisher startForecastingPublisher = (StartForecastingPublisher) ctx.getPublisher(publisherKey);

                        if (startForecastingPublisher == null) {
                            synchronized (this) {
                                startForecastingPublisher = (StartForecastingPublisher) ctx.getPublisher(publisherKey);
                                if (startForecastingPublisher == null) {
                                    log.info("StartForecastingPublisher for method {} not found, creating a new one.", method);
                                    startForecastingPublisher = new StartForecastingPublisher(method, name);
                                    ctx.registerPublisher(startForecastingPublisher);
                                    log.info("New StartForecastingPublisher for method {} registered successfully.", method);
                                }
                            }
                        }

                        startForecastingPublisher.send(startForecastingMessageToMap(startForecastingMessage), metricListMessage.getName());
                        log.info("Start forecasting event published for application {}; method {}; message: {}", name, method, startForecastingMessageToMap(startForecastingMessage));
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
            List<String> metricNames = new java.util.ArrayList<>(metricListMessage.getMetricList().stream()
                    .map(Metric::getName)
                    .toList());

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