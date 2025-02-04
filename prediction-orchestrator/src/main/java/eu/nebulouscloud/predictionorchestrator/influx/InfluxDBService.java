package eu.nebulouscloud.predictionorchestrator.influx;

import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.domain.Organization;
import com.influxdb.client.write.Point;
import eu.nebulouscloud.predictionorchestrator.Prediction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class InfluxDBService {

    private static final Logger log = LoggerFactory.getLogger(InfluxDBService.class);

    private final InfluxDBClient influxDBClient;
    private final String id;
    private final String org;

    public InfluxDBService(
            @Value("${influx.url}") String url,
            @Value("${influx.port}") int port,
            @Value("${influx.token}") String token,
            @Value("${influx.org}") String org,
            @Value("${influx.org.id}") String id
    ) {
        this.id = id;
        this.org = org;
        // Build the connection string (http://<host>:<port>)
        String connectionString = String.format("http://%s:%d", url, port);

        // Create the client with token authentication
        this.influxDBClient = InfluxDBClientFactory.create(
                connectionString,
                token.toCharArray(),
                org
        );

        log.info("InfluxDB Client initialized for org: {}", org);
    }

    /**
     * Writes a Prediction object to InfluxDB, ensuring the bucket exists first.
     *
     * @param appName     The InfluxDB bucket to write to.
     * @param prediction The Prediction to be saved.
     */
    public void writePrediction(String appName, Prediction prediction, String method) {
        String bucketName = "nebulous_" + appName + "_predicted_bucket";
        // 1. Ensure the bucket exists (create if missing).
        createBucketIfNotExists(bucketName);

        // 2. Obtain the blocking Write API (cannot use try-with-resources here).
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        Point point = Point
                .measurement(prediction.getMetricName())
                .addTag("application_name", appName)
                .addTag("forecaster", method)
                .addTag("level", "int32(1)")
                .addField("metricValue", prediction.getMetricValue())
                .time(prediction.getPredictionTime(), WritePrecision.S);

        // 4. Perform the blocking write
        try {
            writeApi.writePoint(bucketName, this.org, point);
            log.info("Successfully wrote prediction to InfluxDB for metric: {} in bucket: {}",
                    prediction.getMetricName(), bucketName);
        } catch (Exception e) {
            log.error("Failed to write prediction to InfluxDB", e);
        }
    }

    /**
     * Checks if the bucket exists in InfluxDB; if not, creates it.
     * Requires that the token has permission to create buckets.
     */
    private void createBucketIfNotExists(String bucketName) {
        try {
            BucketsApi bucketsApi = influxDBClient.getBucketsApi();
            Bucket existing = bucketsApi.findBucketByName(bucketName);
            if (existing == null) {
                // Need org ID to associate the bucket with our org
                OrganizationsApi organizationsApi = influxDBClient.getOrganizationsApi();
                List<Organization> orgs = organizationsApi.findOrganizations();
                Organization organization = orgs.stream()
                        .filter(o -> o.getName().equals(org))
                        .findFirst()
                        .orElse(null);

                if (organization == null) {
                    throw new RuntimeException("Organization not found by name: " + org);
                }

                Bucket newBucket = new Bucket();
                newBucket.setName(bucketName);
                newBucket.setOrgID(organization.getId());

                // Create the bucket
                newBucket = bucketsApi.createBucket(newBucket);

                log.info("Created bucket '{}' in organization '{}'",
                        newBucket.getName(), organization.getName());
            } else {
                log.debug("Bucket '{}' already exists in organization '{}'",
                        bucketName, id);
            }
        } catch (Exception e) {
            log.error("Error creating or finding bucket '{}': {}", bucketName, e.getMessage());
        }
    }
}
