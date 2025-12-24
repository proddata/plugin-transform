package io.kestra.plugin.transform;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;

public interface ExpressionEngine {
    IonValue evaluate(String expression, IonStruct record) throws ExpressionException;
}
