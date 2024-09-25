package eu.nebulouscloud.predictionorchestrator.influx;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import eu.nebulouscloud.predictionorchestrator.Prediction;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import static org.slf4j.LoggerFactory.getLogger;


@Service
public class InfluxDBService {

    private static final Logger log = LoggerFactory.getLogger(InfluxDBService.class);

    private final InfluxDBClient influxDBClient;
    private final String org;

    public InfluxDBService(
            @Value("${influx.url}") String url,
            @Value("${influx.port}") int port,
            @Value("${influx.username}") String username,
            @Value("${influx.password}") String password,
            @Value("${influx.org}") String org) {
        this.org = org;

        // Construct the URL with the port
        String connectionString = String.format("http://%s:%d", url, port);

        // Use the username and password for authentication
        this.influxDBClient = InfluxDBClientFactory.create(connectionString, username.toCharArray(), password, org);
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

            // Ensure that you are using the instance variable `org`
            writeApi.writePoint(bucket, this.org, point);
            log.info("Successfully wrote prediction to InfluxDB for metric: {} in bucket: {}", prediction.getMetricName(), bucket);
        } catch (Exception e) {
            log.error("Failed to write prediction to InfluxDB", e);
        }
    }
}