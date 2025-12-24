package io.kestra.plugin.transform;

import com.amazon.ion.IonStruct;

import java.util.List;

public interface TransformTaskEngine {
    TransformResult execute(List<IonStruct> records) throws TransformException;
}
