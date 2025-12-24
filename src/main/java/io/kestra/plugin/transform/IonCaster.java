package io.kestra.plugin.transform;

import com.amazon.ion.IonValue;

public interface IonCaster {
    IonValue cast(IonValue value, IonTypeName targetType) throws CastException;
}
