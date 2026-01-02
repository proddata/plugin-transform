package io.kestra.plugin.transform.expression;

import com.amazon.ion.IonStruct;
import com.amazon.ion.IonValue;
import io.kestra.plugin.transform.ion.IonValueUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

class DefaultExpressionEngineTest {
    @Test
    void returnsNullForBlankExpression() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        IonValue value = engine.evaluate("   ", record);

        assertThat(IonValueUtils.isNull(value), is(true));
    }

    @Test
    void rejectsInvalidExpression() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("user.", record)
        );

        assertThat(exception.getMessage(), containsString("Invalid expression: user."));
    }

    @Test
    void rejectsArrayExpansionOnNonList() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        record.put("items", IonValueUtils.system().newString("oops"));

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("items[].price", record)
        );

        assertThat(exception.getMessage(), containsString("Expected list for segment 'items[]'"));
    }

    @Test
    void rejectsUnknownFunction() {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();

        ExpressionException exception = Assertions.assertThrows(
            ExpressionException.class,
            () -> engine.evaluate("missingFn(1)", record)
        );

        assertThat(exception.getMessage(), containsString("Unknown function: missingFn"));
    }

    @Test
    void resolvesBracketFieldAccess() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        IonStruct user = IonValueUtils.system().newEmptyStruct();
        user.put("first name", IonValueUtils.system().newString("Anna"));
        record.put("user", user);

        IonValue value = engine.evaluate("user[\"first name\"]", record);

        assertThat(IonValueUtils.toJavaValue(value), is("Anna"));
    }

    @Test
    void resolvesBracketFieldAccessOnPositionalScope() throws Exception {
        DefaultExpressionEngine engine = new DefaultExpressionEngine();
        IonStruct record = IonValueUtils.system().newEmptyStruct();
        IonStruct row = IonValueUtils.system().newEmptyStruct();
        row.put("field name", IonValueUtils.system().newInt(1));
        record.put("$1", row);

        IonValue value = engine.evaluate("$1[\"field name\"]", record);

        assertThat(IonValueUtils.toJavaValue(value), is(1L));
    }
}
