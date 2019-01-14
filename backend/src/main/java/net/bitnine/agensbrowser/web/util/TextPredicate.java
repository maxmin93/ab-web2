package net.bitnine.agensbrowser.web.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import java.util.function.BiPredicate;

public class TextPredicate implements BiPredicate<Object, Object> {

    private Mode mode;
    enum Mode {
        CONTAINS, NOTCONTAINS, STARTSWITH, ENDSWITH
    }

    public TextPredicate() {
        this.mode = Mode.CONTAINS;
    };
    public TextPredicate(Mode mode) {
        this.mode = mode;
    };

    @Override
    public boolean test(final Object first, final Object second) {
        switch (mode) {
            case CONTAINS:
                return first.toString().contains(second.toString());
            case NOTCONTAINS:
                return !first.toString().contains(second.toString());
            case STARTSWITH:
                return first.toString().startsWith(second.toString());
            case ENDSWITH:
                return first.toString().endsWith(second.toString());
        }
        return false;
    }

    public static P<Object> contains(Object pattern) {
        BiPredicate<Object, Object> b = new TextPredicate(Mode.CONTAINS);
        return new P<Object>(b, pattern);
    }

    public static P<Object> notContains(Object pattern) {
        BiPredicate<Object, Object> b = new TextPredicate(Mode.NOTCONTAINS);
        return new P<Object>(b, pattern);
    }

    public static P<Object> startsWith(Object pattern) {
        BiPredicate<Object, Object> b = new TextPredicate(Mode.STARTSWITH);
        return new P<Object>(b, pattern);
    }

    public static P<Object> endsWith(Object pattern) {
        BiPredicate<Object, Object> b = new TextPredicate(Mode.ENDSWITH);
        return new P<Object>(b, pattern);
    }

}
