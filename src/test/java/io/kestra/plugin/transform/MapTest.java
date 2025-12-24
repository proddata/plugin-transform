package io.kestra.plugin.transform;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest
class MapTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void mapsFieldsAndCasts() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "user", java.util.Map.of("id", "cust-1"),
            "createdAt", "2024-01-01T00:00:00Z",
            "items", List.of(
                java.util.Map.of("price", new BigDecimal("10.50")),
                java.util.Map.of("price", new BigDecimal("5.25"))
            )
        );

        Map task = Map.builder()
            .from(Property.ofValue(List.of(record)))
            .fields(java.util.Map.of(
                "customer_id", Map.FieldDefinition.builder().expr("user.id").type(IonTypeName.STRING).build(),
                "created_at", Map.FieldDefinition.builder().expr("createdAt").type(IonTypeName.TIMESTAMP).build(),
                "total", Map.FieldDefinition.builder().expr("sum(items[].price)").type(IonTypeName.DECIMAL).build()
            ))
            .options(Map.Options.builder().dropNulls(true).onError(TransformOptions.OnErrorMode.FAIL).build())
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.get("customer_id"), is("cust-1"));
        assertThat(mapped.get("created_at"), is("2024-01-01T00:00:00Z"));
        assertThat(mapped.get("total"), is(new BigDecimal("15.75")));
    }

    @Test
    void skipsOnErrorAndReportsStats() throws Exception {
        java.util.Map<String, Object> ok = java.util.Map.of("value", 1);
        java.util.Map<String, Object> bad = java.util.Map.of();

        Map task = Map.builder()
            .from(Property.ofValue(List.of(ok, bad)))
            .fields(java.util.Map.of(
                "value", Map.FieldDefinition.builder().expr("value").type(IonTypeName.INT).build()
            ))
            .options(Map.Options.builder().onError(TransformOptions.OnErrorMode.SKIP).build())
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        assertThat(output.getRecords(), hasSize(1));
        assertThat(output.getStats().processed(), is(2));
        assertThat(output.getStats().failed(), is(1));
        assertThat(output.getStats().dropped(), is(1));
        assertThat(output.getStats().fieldErrors().size(), is(1));
    }

    @Test
    void supportsShorthandAndOptionalType() throws Exception {
        java.util.Map<String, Object> record = java.util.Map.of(
            "user", java.util.Map.of("id", "u-1"),
            "active", "true"
        );

        Map task = Map.builder()
            .from(Property.ofValue(List.of(record)))
            .fields(java.util.Map.of(
                "customer_id", Map.FieldDefinition.from("user.id"),
                "active_raw", Map.FieldDefinition.builder().expr("active").build()
            ))
            .build();

        RunContext runContext = runContextFactory.of(java.util.Map.of());
        Map.Output output = task.run(runContext);

        java.util.Map<String, Object> mapped = (java.util.Map<String, Object>) output.getRecords().getFirst();
        assertThat(mapped.get("customer_id"), is("u-1"));
        assertThat(mapped.get("active_raw"), is("true"));
    }
}
