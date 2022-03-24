package com.example.spannerspringbootr2jdbcapp;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.r2dbc.v2.JsonWrapper;
import io.r2dbc.spi.*;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import uk.org.webcompere.systemstubs.environment.EnvironmentVariables;
import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Testcontainers
@ExtendWith(SystemStubsExtension.class)
@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("it")
@DirtiesContext
public class SpannerIT {

    static final String PROJECT_ID = "nv-local";
    static final String INSTANCE_ID = "test-instance";
    static final String DATABASE_NAME = "trades";

    @Container
    private static final SpannerEmulatorContainer spannerContainer =
        new SpannerEmulatorContainer(
            DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator").withTag("1.4.1"));

    @SystemStub
    private static EnvironmentVariables environmentVariables;

    @Autowired
    ConnectionFactory connectionFactory;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        environmentVariables.set("SPANNER_EMULATOR_HOST", spannerContainer.getEmulatorGrpcEndpoint());
        r.add("spring.r2dbc.url", () -> "r2dbc:cloudspanner://" +
            "/projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID + "/databases/" + DATABASE_NAME);
    }

    @SneakyThrows
    @BeforeAll
    static void beforeAll() {
        init();
    }

    @Test
    void test() {
        StepVerifier.create(
                Mono.from(connectionFactory.create())
                    .flatMap(this::createTable)
                    .flatMap(r -> Mono.from(r.getRowsUpdated()))
            )
            .expectNextMatches(m -> m == 0)
            .verifyComplete();

        StepVerifier.create(
                Mono.from(connectionFactory.create())
                    .flatMap(this::saveBooks)
                    .flatMap(this::retrieveBooks)
            )
            .expectNextMatches(m ->
                m.size() == 3)
            .verifyComplete();
    }

    public Mono<Result> createTable(Connection connection) {
        return Mono.from(connection.createStatement("CREATE TABLE BOOKS ("
                + "  ID STRING(20) NOT NULL,"
                + "  TITLE STRING(MAX) NOT NULL,"
                + "  EXTRADETAILS JSON"
                + ") PRIMARY KEY (ID)").execute());
    }

    /**
     * Saves two books.
     */
    public Mono<Connection> saveBooks(Connection connection) {
        io.r2dbc.spi.Statement statement = connection.createStatement(
                "INSERT BOOKS "
                    + "(ID, TITLE)"
                    + " VALUES "
                    + "(@id, @title)")
            .bind("id", "book1")
            .bind("title", "Book One")
            .add()
            .bind("id", "book2")
            .bind("title", "Book Two");


        io.r2dbc.spi.Statement statement2 = connection.createStatement(
                "INSERT BOOKS "
                    + "(ID, TITLE, EXTRADETAILS)"
                    + " VALUES "
                    + "(@id, @title, @extradetails)")
            .bind("id", "book3")
            .bind("title", "Book Three")
            .bind("extradetails", new JsonWrapper("{\"rating\":9,\"series\":true}"));

        return Flux.concat(
                connection.beginTransaction(),
                Flux.concat(statement.execute(), statement2.execute())
                    .flatMapSequential(r -> Mono.from(r.getRowsUpdated()))
                    .then(),
                connection.commitTransaction())
            .doOnComplete(() -> System.out.println("Insert books transaction committed."))
            .collectList()
            .flatMap(x -> Mono.just(connection));
    }

    /**
     * Finds books in the table named BOOKS.
     */
    public Mono<List<String>> retrieveBooks(Connection connection) {
        return Flux.from(connection.createStatement("SELECT * FROM books").execute())
            .flatMap(
                spannerResult ->
                    spannerResult.map(
                        (Row r, RowMetadata meta) -> describeBook(r)))
            .doOnNext(System.out::println)
            .collectList();
    }

    public String describeBook(Row r) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder
            .append("Retrieved book: ")
            .append(r.get("ID", String.class))
            .append("; Title: ")
            .append(r.get("TITLE", String.class));
        if (r.get("EXTRADETAILS", JsonWrapper.class) != null) {
            stringBuilder
                .append("; Extra Details: ")
                .append(r.get("EXTRADETAILS", JsonWrapper.class).toString());
        }
        return stringBuilder.toString();
    }

    private static void createInstance(Spanner spanner) throws InterruptedException, ExecutionException {
        InstanceConfigId instanceConfig = InstanceConfigId.of(PROJECT_ID, "emulator-config");
        InstanceId instanceId = InstanceId.of(PROJECT_ID, INSTANCE_ID);
        InstanceAdminClient insAdminClient = spanner.getInstanceAdminClient();
        insAdminClient.createInstance(InstanceInfo.newBuilder(instanceId).setNodeCount(1).setDisplayName("Test instance").setInstanceConfigId(instanceConfig).build()).get();
    }

    private static void createDatabase(Spanner spanner) throws InterruptedException, ExecutionException {
        DatabaseAdminClient dbAdminClient = spanner.getDatabaseAdminClient();
        dbAdminClient.createDatabase(INSTANCE_ID, DATABASE_NAME, List.of()).get();
    }

    @SneakyThrows
    private static void init() {
        SpannerOptions options = SpannerOptions.newBuilder()
            .setEmulatorHost(spannerContainer.getEmulatorGrpcEndpoint())
            .setCredentials(NoCredentials.getInstance())
            .setProjectId(PROJECT_ID)
            .build();

        Spanner spanner = options.getService();
        createInstance(spanner);
        createDatabase(spanner);
    }
}
