package eu.nebulouscloud.predictionorchestrator.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import eu.nebulouscloud.predictionorchestrator.Prediction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Service
@Slf4j
public class InfluxDBService {

    private final InfluxDBClient influxDBClient;
    private final String org;

    public InfluxDBService(
            @Value("${influx.url}") String url,
            @Value("${influx.token}") String token,
            @Value("${influx.org}") String org) {
        this.org = org;
        this.influxDBClient = InfluxDBClientFactory.create(url, token.toCharArray());
    }

    public Map<String, Prediction> fetchPredictions(String bucket, String metricName, LocalDateTime from, LocalDateTime to) {
        String fluxQuery = String.format(
                "from(bucket:\"%s\") |> range(start: %s, stop: %s) |> filter(fn: (r) => r._measurement == \"predictions\" and r[\"metric_name\"] == \"%s\")",
                bucket,
                from.format(DateTimeFormatter.ISO_DATE_TIME),
                to.format(DateTimeFormatter.ISO_DATE_TIME),
                metricName
        );

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = queryApi.query(fluxQuery, org);

        Map<String, Prediction> predictionsByMethod = new HashMap<>();

        for (FluxTable table : tables) {
            for (FluxRecord record : table.getRecords()) {
                // Extract data from the record to create a Prediction object
                String method = record.getValueByKey("prediction_method").toString();
                double metricValue = ((Number) record.getValue()).doubleValue();
                String componentId = record.getValueByKey("component_id").toString();
                long timestamp = record.getTime().toEpochMilli();
                double probability = ((Number) record.getValueByKey("probability")).doubleValue();
                String[] confidenceIntervalStrings = record.getValueByKey("confidence_interval").toString().split(",");
                List<Double> confidenceInterval = new ArrayList<>();
                for (String value : confidenceIntervalStrings) {
                    confidenceInterval.add(Double.parseDouble(value.trim()));
                }
                long predictionTime = ((Number) record.getValueByKey("prediction_time")).longValue();

                Prediction prediction = new Prediction(
                        metricValue,
                        componentId,
                        timestamp,
                        probability,
                        confidenceInterval,
                        predictionTime,
                        method,
                        metricName
                );

                // Store the prediction by method
                predictionsByMethod.put(method, prediction);
            }
        }

        return predictionsByMethod;
    }

    public void writePrediction(String bucket, Prediction prediction) {
        try (WriteApi writeApi = influxDBClient.getWriteApi()) {
            Point point = Point.measurement("predictions")
                    .addTag("metric_name", prediction.getMetricName())
                    .addTag("prediction_method", prediction.getPredictionMethod())
                    .addTag("component_id", prediction.getComponentId())
                    .addField("metric_value", prediction.getMetricValue())
                    .addField("probability", prediction.getProbability())
                    .addField("confidence_interval", prediction.getConfidenceInterval().toString()) // Adjust as needed for InfluxDB
                    .addField("prediction_time", prediction.getPredictionTime())
                    .time(prediction.getPredictionTime(), WritePrecision.MS);

            writeApi.writePoint(bucket, org, point);
            log.info("Successfully wrote prediction to InfluxDB for metric: {} in bucket: {}", prediction.getMetricName(), bucket);
        } catch (Exception e) {
            log.error("Failed to write prediction to InfluxDB", e);
        }
    }

    public void close() {
        influxDBClient.close();
    }
}