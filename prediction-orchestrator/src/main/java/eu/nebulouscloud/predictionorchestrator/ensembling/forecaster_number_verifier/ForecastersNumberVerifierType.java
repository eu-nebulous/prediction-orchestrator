package eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier;

public enum ForecastersNumberVerifierType {
    STATIC_FORECASTERS_COUNT_NEEDED("staticForecastersCountNeeded"),

    PERCENTAGE_FORECASTERS_COUNT_NEEDED("percentageForecastersCountNeeded");

    private String type;

    ForecastersNumberVerifierType(String type) {
        this.type = type;
    }
}
