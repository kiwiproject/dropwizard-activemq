package org.kiwiproject.dropwizard.activemq.util;

import static java.lang.invoke.MethodType.methodType;
import static org.kiwiproject.base.KiwiPreconditions.checkArgumentNotNull;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Objects;

@UtilityClass
@Slf4j
public class Utils {

    private static final String DEFAULT_CLOSE_METHOD_NAME = "close";

    @FunctionalInterface
    public interface FunctionThrowsException<T, R> {
        R apply(T t) throws Exception;
    }

    @FunctionalInterface
    public interface RunnableThrowsException {
        void run() throws Exception;
    }

    /**
     * Silently call runnable#run, suppressing any Throwables.
     *
     * @param runnable the runnable to run
     */
    public static void silentlyRun(RunnableThrowsException runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            LOG.debug("Suppressed exception: "+ e);
        }
    }

    /**
     * Silently call runnable#run on each of the given runnables, suppressing any Throwables.
     *
     * @param runnables the runnables to run
     */
    public static void silentlyRun(RunnableThrowsException... runnables) {
        Arrays.stream(runnables).forEach(Utils::silentlyRun);
    }

    /**
     * Safely closes all passed resources, which may be null.
     *
     * @param instances the instances to close, or a {@link Pair} with an instance and the name of its "close" method
     */
    public static void safelyClose(Object... instances) {
        Arrays.stream(instances)
                .filter(Objects::nonNull)
                .map(Utils::toPairOfInstanceAndCloseMethodName)
                .forEach(Utils::closeResource);
    }

    private static Pair<?, ?> toPairOfInstanceAndCloseMethodName(Object instance) {
        return (instance instanceof Pair) ?
                (Pair<?, ?>) instance : Pair.of(instance, DEFAULT_CLOSE_METHOD_NAME);
    }

    private static void closeResource(Pair<?, ?> pair) {
        try {
            checkArgumentNotNull(pair.getLeft(), "pair.left must be a non-null Object to close");
            checkArgumentNotNull(pair.getRight(), "pair.right must be a non-null close-method name");

            var instance = pair.getLeft();
            var closeMethodName = pair.getRight().toString();
            var methodHandle = MethodHandles.lookup()
                    .findVirtual(instance.getClass(), closeMethodName, methodType(Void.TYPE));
            methodHandle.invoke(instance);
        } catch (Throwable t) {
            LOG.trace("Suppressed exception:", t);
        }
    }
}
