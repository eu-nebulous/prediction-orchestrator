package eu.nebulouscloud.predictionorchestrator.consumers;

import eu.nebulouscloud.exn.core.Consumer;
import eu.nebulouscloud.exn.core.Context;
import eu.nebulouscloud.exn.core.Handler;
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

        @Override
        public void onMessage(String key, String address, Map body, Message message, Context ctx) {
            // Extracting data from the body map
            String name = (String) body.get("name");
            long version = (Long) body.get("version");
            log.info("Metric list received for application " + name + " with version " + version);

            List<Map<String, String>> metricsMap = (List<Map<String, String>>) body.get("metric_list");

            // Converting maps to Metric objects
            List<Metric> metrics = metricsMap.stream()
                    .map(m -> new Metric(m.get("name"), m.get("upper_bound"), m.get("lower_bound")))
                    .collect(Collectors.toList());

            // Creating MetricListMessage
            MetricListMessage metricListMessage = new MetricListMessage(name, version, metrics);

            // Example values for the additional parameters
            long timestamp = System.currentTimeMillis() / 1000;
            long epochStart = timestamp - 100; // Example start time
            int numberOfForwardPredictions = 5;
            int predictionHorizon = 120;

            // Mapping to StartForecastingMessage
            StartForecastingMessage startForecastingMessage = mapToStartForecastingMessage(metricListMessage,
                    timestamp, epochStart,
                    numberOfForwardPredictions, predictionHorizon);


            String methodName = "exponentialsmoothing";
            String publisherKey = "start_forecasting_" + name;
            StartForecastingPublisher startForecastingPublisher = (StartForecastingPublisher) ctx.getPublisher(publisherKey);

            if (startForecastingPublisher == null) {
                log.info("StartForecastingPublisher for method {} not found, creating a new one.", methodName);
                startForecastingPublisher = new StartForecastingPublisher("exponentialsmoothing", name);
                ctx.registerPublisher(startForecastingPublisher);
                log.info("New StartForecastingPublisher for method {}", methodName);
            }
            startForecastingPublisher.send(startForecastingMessageToMap(startForecastingMessage),metricListMessage.getName());
            log.info("Start forecasting event published for application {}; message:" + startForecastingMessageToMap(startForecastingMessage), name);
        }

    }
    public static StartForecastingMessage mapToStartForecastingMessage(MetricListMessage metricListMessage,
                                                                       long timestamp, long epochStart,
                                                                       int numberOfForwardPredictions, int predictionHorizon) {
        List<String> metricNames = metricListMessage.getMetricList().stream()
                .map(Metric::getName)
                .toList();

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

