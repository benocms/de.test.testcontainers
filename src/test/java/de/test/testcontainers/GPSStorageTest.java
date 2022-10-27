package de.test.testcontainers;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Rule;
import org.junit.jupiter.api.*;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static java.nio.charset.StandardCharsets.UTF_8;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // Share data between the test methods
@TestMethodOrder(MethodOrderer.OrderAnnotation.class) // Enable ordering of test methods
@Testcontainers
class GPSStorageTest {

    private static final String BUCKET_NAME = "BUCKET_TEST";

    private static final Integer INTERNAL_PORT = 8888;

    protected static Storage storage = null;

    // Set the GenericContainer to 'static' and the GenericContainer will be shared between test methods
    @Container
    private static final GenericContainer<?>gcsFakeContainer =
            new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:latest"))
            .withCommand("-port",INTERNAL_PORT.toString(), "-scheme", "http")
            .withExposedPorts(8888)
                    .waitingFor(Wait.forHttp("/").forStatusCode(404))
            .withReuse(true);

    @Rule
    public static void setup() {
        int mappedPort = gcsFakeContainer.getMappedPort(INTERNAL_PORT);
        StorageOptions storageOps = StorageOptions.newBuilder()
                .setCredentials(NoCredentials.getInstance())
                .setHost("http://" + gcsFakeContainer.getHost() + ":" + mappedPort)
                .setProjectId("TEST_LOCAL")
                .build();

        storage = storageOps.getService();
    }

    @Test
    @Order(1)
    public void testCreateBucket() {
        createBucket();
        Assertions.assertTrue(checkBucketExists());
    }

    @Test
    @Order(2)
    public void testIfFileWritten() {

        String filename = "test.txt";
        Bucket bucket = storage.get(BUCKET_NAME);
        bucket.create(filename, "Hello, World!".getBytes(UTF_8));
        Assertions.assertTrue(fileExits(filename));
    }

    private boolean fileExits(String filename) {
        return storage.get(BUCKET_NAME,filename) != null;
    }

    private void createBucket() {
        try {
            storage.create(BucketInfo.of(BUCKET_NAME));
        } catch (Exception ex) {
            System.err.println("Error creating bucket");
            ex.printStackTrace();
        }
    }

    private boolean checkBucketExists() {
        return storage.get(BUCKET_NAME, Storage.BucketGetOption.fields()) != null;
    }
}