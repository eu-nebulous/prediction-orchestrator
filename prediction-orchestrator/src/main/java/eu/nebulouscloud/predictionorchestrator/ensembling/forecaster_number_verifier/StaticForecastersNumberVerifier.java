package eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class StaticForecastersNumberVerifier extends ForecastersNumberVerifier {

    private final double staticThreshold;

    @Override
    public boolean isDataSufficient(int numberOfValidData, int expectedForecastersDataCount) {
        return numberOfValidData >= staticThreshold;
    }
}
