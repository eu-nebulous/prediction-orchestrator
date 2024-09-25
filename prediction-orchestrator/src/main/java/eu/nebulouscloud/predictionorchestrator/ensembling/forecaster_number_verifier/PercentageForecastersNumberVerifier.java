package eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class PercentageForecastersNumberVerifier extends ForecastersNumberVerifier {

    private final double percentageThreshold;

    @Override
    public boolean isDataSufficient(int numberOfValidData, int expectedForecastersDataCount) {
        return (((double) numberOfValidData) / ((double) expectedForecastersDataCount))
                > percentageThreshold;
    }
}
