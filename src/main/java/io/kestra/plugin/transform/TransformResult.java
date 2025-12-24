package io.kestra.plugin.transform;

import com.amazon.ion.IonStruct;

import java.util.List;

public record TransformResult(
    List<IonStruct> records,
    TransformStats stats
) {
}
