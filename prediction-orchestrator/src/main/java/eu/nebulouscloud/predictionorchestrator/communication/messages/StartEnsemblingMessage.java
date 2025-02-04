package eu.nebulouscloud.predictionorchestrator.communication.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class StartEnsemblingMessage {
    private List<MetricInfo> metrics;
    private List<String> models;

}