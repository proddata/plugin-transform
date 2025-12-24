package io.kestra.plugin.transform;

import com.amazon.ion.IonStruct;

public interface RecordTransformer {
    IonStruct transform(IonStruct input) throws TransformException;
}
