package io.kestra.plugin.transform;

import io.kestra.core.junit.annotations.ExecuteFlow;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.executions.TaskRun;
import io.kestra.core.models.flows.State;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class MapFlowTest {
    @Test
    @ExecuteFlow("flows/map_flow.yaml")
    void executesFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("map");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();
        List<Map<String, Object>> records = (List<Map<String, Object>>) outputs.get("records");

        assertThat(records.size(), is(1));
        Map<String, Object> record = records.getFirst();
        assertThat(record.get("customer_id"), is("u-1"));
        assertThat(record.get("created_at"), is("2024-01-01T00:00:00Z"));
        assertThat(record.get("total").toString(), is("15.25"));
    }

    @Test
    @ExecuteFlow("flows/map_flow_store.yaml")
    void executesStoreFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("map");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();

        assertThat(outputs.containsKey("records"), is(false));
        Object uri = outputs.get("uri");
        assertThat(uri != null, is(true));
        assertThat(uri.toString().startsWith("kestra://"), is(true));
    }

    @Test
    @ExecuteFlow("flows/unnest_flow_store.yaml")
    void executesUnnestStoreFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("explode");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();

        assertThat(outputs.containsKey("records"), is(false));
        Object uri = outputs.get("uri");
        assertThat(uri != null, is(true));
        assertThat(uri.toString().startsWith("kestra://"), is(true));
    }

    @Test
    @ExecuteFlow("flows/filter_flow_store.yaml")
    void executesFilterStoreFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("filter");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();

        assertThat(outputs.containsKey("records"), is(false));
        Object uri = outputs.get("uri");
        assertThat(uri != null, is(true));
        assertThat(uri.toString().startsWith("kestra://"), is(true));
    }

    @Test
    @ExecuteFlow("flows/aggregate_flow_store.yaml")
    void executesAggregateStoreFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("aggregate");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();

        assertThat(outputs.containsKey("records"), is(false));
        Object uri = outputs.get("uri");
        assertThat(uri != null, is(true));
        assertThat(uri.toString().startsWith("kestra://"), is(true));
    }

    @Test
    @ExecuteFlow("flows/zip_flow.yaml")
    void executesZipFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("zip");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();
        List<Map<String, Object>> records = (List<Map<String, Object>>) outputs.get("records");

        assertThat(records.size(), is(2));
        Map<String, Object> record = records.getFirst();
        assertThat(record.get("id"), is("a"));
        assertThat(record.get("status"), is("ok"));
    }

    @Test
    @ExecuteFlow("flows/zip_flow_store.yaml")
    void executesZipStoreFlow(Execution execution) {
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));

        List<TaskRun> taskRuns = execution.findTaskRunsByTaskId("zip");
        TaskRun taskRun = taskRuns.getFirst();
        Map<String, Object> outputs = (Map<String, Object>) taskRun.getOutputs();

        assertThat(outputs.containsKey("records"), is(false));
        Object uri = outputs.get("uri");
        assertThat(uri != null, is(true));
        assertThat(uri.toString().startsWith("kestra://"), is(true));
    }
}
