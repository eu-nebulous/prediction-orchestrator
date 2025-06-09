package eu.nebulouscloud.predictionorchestrator.communication.messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricInfo {
    private String metric;
}