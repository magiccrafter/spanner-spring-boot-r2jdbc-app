package com.example.spannerspringbootr2jdbcapp;

import lombok.Data;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.core.query.CriteriaDefinition;
import org.springframework.data.relational.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.testcontainers.containers.SpannerEmulatorContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.test.StepVerifier;

@ExtendWith(SpringExtension.class)
@SpringBootTest
@ActiveProfiles("it")
@DirtiesContext
public class SpannerIT {

    static final String PROJECT_ID = "emulator-config";
    static final String INSTANCE_ID = "test-instance";
    static final String DATABASE_NAME = "test-database";

    static SpannerEmulatorContainer spannerContainer;

    @Autowired
    private R2dbcEntityTemplate template;

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry r) {
        r.add("spring.cloud.gcp.spanner.emulator-host", spannerContainer::getEmulatorGrpcEndpoint);
        r.add("spring.r2dbc.url", () -> "r2dbc:cloudspanner://" + spannerContainer.getEmulatorHttpEndpoint() +
            "/projects/" + PROJECT_ID + "/instances/" + INSTANCE_ID + "/databases/" + DATABASE_NAME);
    }

    @BeforeAll
    public static void beforeAll() {
        spannerContainer = new SpannerEmulatorContainer(
            DockerImageName.parse("gcr.io/cloud-spanner-emulator/emulator").withTag("1.4.1"));
        spannerContainer.start();
    }

    @AfterAll
    public static void afterAll() {
        spannerContainer.stop();
    }

    @Test
    void test() {
        StepVerifier.create(
                template.select(Query.query(CriteriaDefinition.empty()), SomeClazz.class)
            )
            .verifyComplete();

    }

    @Data
    @Table("test")
    public class SomeClazz {

        @Column("column")
        private String column;
    }
}
