package eu.nebulouscloud.predictionorchestrator.ensembling;

import eu.nebulouscloud.predictionorchestrator.Properties;
import eu.nebulouscloud.predictionorchestrator.ensembling.ensembler.AverageValuesEnsembler;
import eu.nebulouscloud.predictionorchestrator.ensembling.ensembler.Ensembler;
import eu.nebulouscloud.predictionorchestrator.ensembling.ensembler.EnsemblerService;
import eu.nebulouscloud.predictionorchestrator.ensembling.ensembler.OuterEnsembler;
import eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier.ForecastersNumberVerifier;
import eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier.PercentageForecastersNumberVerifier;
import eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier.StaticForecastersNumberVerifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
public class EnsemblingMechanismFactory {

    public static EnsemblingMechanism getEnsemblingMechanism(Properties properties) {
        Ensembler ensembler;
        ForecastersNumberVerifier forecastersNumberVerifier;

        switch (properties.getForecastersNumberVerifierType()) {
            case STATIC_FORECASTERS_COUNT_NEEDED: {
                forecastersNumberVerifier =
                        new StaticForecastersNumberVerifier(properties.getForecasterNumberThreshold());
                break;
            }
            case PERCENTAGE_FORECASTERS_COUNT_NEEDED: {
                forecastersNumberVerifier =
                        new PercentageForecastersNumberVerifier(properties.getForecasterNumberThreshold());
                        break;
            }
            default: {
                throw new IllegalArgumentException("Pooling strategy not present in the system");
            }
        }

        switch (properties.getEnsemblerType()) {
            case AVERAGE: {
                ensembler = new AverageValuesEnsembler();
                break;
            }
            case OUTER: {
                ensembler = new OuterEnsembler(new EnsemblerService(
                        properties.getEnsemblerBaseUrl(),
                        properties.getEnsemblerUri()
                ));
                break;
            }
            default: {
                throw new IllegalArgumentException("Pooling strategy not present in the system");
            }
        }

        return new EnsemblingMechanism(ensembler, forecastersNumberVerifier);
    }
}
