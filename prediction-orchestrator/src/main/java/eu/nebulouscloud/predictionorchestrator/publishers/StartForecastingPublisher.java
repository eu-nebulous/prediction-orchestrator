package eu.nebulouscloud.predictionorchestrator.publishers;

import eu.nebulouscloud.exn.core.Publisher;

public class StartForecastingPublisher extends Publisher {
    String topicName;
    public StartForecastingPublisher(String topicName, String applicationName) {
        super("start_forecasting_" + applicationName, "eu.nebulouscloud.forecasting.start_forecasting." + topicName, true, true);
        this.topicName = topicName;
    }
}
