package eu.nebulouscloud.predictionorchestrator;


import eu.nebulouscloud.predictionorchestrator.ensembling.ensembler.EnsemblerType;
import eu.nebulouscloud.predictionorchestrator.ensembling.forecaster_number_verifier.ForecastersNumberVerifierType;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("file:predictionorchestrator.properties")
@Getter
@Setter
@ToString
public class Properties {

    @Value("${forecasting_configuration.initial_prediction_horizon}")
    private int initial_prediction_horizon;

    @Value("${forecasting_configuration.initial_forward_prediction_number}")
    private int initial_forward_prediction_number;

    @Value("${ensembling.forecasterNumberVerifier.type}")
    private ForecastersNumberVerifierType forecastersNumberVerifierType;

    @Value("${ensembling.forecasterNumberVerifier.threshold}")
    private double forecasterNumberThreshold;

    @Value("${ensembling.ensemblerType}")
    private EnsemblerType ensemblerType;

    @Value("${ensembler.base-url}")
    private String ensemblerBaseUrl;

    @Value("${ensembler.uri}")
    private String ensemblerUri;

    @Value("${influx.url}")
    private String url;

    @Value("${influx.token}")
    String token;

    @Value("${influx.org}")
    private String org;

}
