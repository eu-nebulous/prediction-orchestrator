package eu.nebulouscloud.predictionorchestrator.communication.publishers;


import eu.nebulouscloud.exn.core.Publisher;

public class StartForecastingPublisher extends Publisher {
    String methodName;
    public StartForecastingPublisher(String methodName, String applicationName) {
        super("start_forecasting_" + methodName + applicationName, "eu.nebulouscloud.forecasting.start_forecasting." + methodName, true, true);
        this.methodName = methodName;
    }
}