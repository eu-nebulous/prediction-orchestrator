package eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier;

public abstract class ForecastersNumberVerifier {

    abstract public boolean isDataSufficient(int numberOfValidData, int expectedForecastersDataCount);
}
