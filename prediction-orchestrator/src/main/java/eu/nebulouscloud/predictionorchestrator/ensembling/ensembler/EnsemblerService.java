package eu.nebulouscloud.predictionorchestrator.ensembling.ensembler;

import eu.nebulouscloud.predictionorchestrator.communication.messages.PredictionsEnsembledMessage;
import eu.nebulouscloud.predictionorchestrator.communication.messages.PredictionsToEnsembleMessage;
import lombok.extern.slf4j.Slf4j;

import org.springframework.http.MediaType;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.UnknownHttpStatusCodeException;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;


@Slf4j
public class EnsemblerService {

    private String ensemblerUri;

    private final WebClient client;

    public EnsemblerService(String baseUrl, String ensemblerUri) {
        //http client is only used for requests logging
        HttpClient httpClient = HttpClient.create()
                .wiretap(true);
        this.ensemblerUri = ensemblerUri;
        this.client = WebClient.builder().baseUrl(baseUrl)
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }

    public PredictionsEnsembledMessage ensemble(PredictionsToEnsembleMessage predictionsToEnsembleMessage) {
        try {
            return client.post()
                    .uri(ensemblerUri)
                    .bodyValue(predictionsToEnsembleMessage)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .onStatus(
                            status -> status.is4xxClientError() || status.is5xxServerError(),
                            response -> Mono.empty()
                    )
                    .bodyToMono(PredictionsEnsembledMessage.class)
                    .block();
        } catch (UnknownHttpStatusCodeException e) {
            log.error("Unknown HTTP status code received: {}", e.getRawStatusCode());
            return null;
        } catch (RuntimeException e) {
            log.error(e.getMessage());
            return null;
        }
    }
}
