package org.kiwiproject.dropwizard.activemq.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

@DisplayName("Utils")
class UtilsTest {

    @Nested
    class SilentlyRun {

        @Nested
        class WithSingleRunnable {

            @Test
            void shouldRunTheRunnable() {
                var ran = new ArrayList<String>();
                assertThatCode(() -> Utils.silentlyRun(() -> ran.add("ran"))).doesNotThrowAnyException();
                assertThat(ran).containsExactly("ran");
            }

            @Test
            void shouldSuppressExceptions() {
                assertThatCode(() -> Utils.silentlyRun(() -> {
                    throw new RuntimeException("oops");
                })).doesNotThrowAnyException();
            }
        }

        @Nested
        class WithMultipleRunnables {

            @Test
            void shouldRunAllRunnables() {
                var ran = new ArrayList<String>();
                assertThatCode(() -> Utils.silentlyRun(
                        () -> ran.add("first"),
                        () -> ran.add("second"),
                        () -> ran.add("third")
                )).doesNotThrowAnyException();
                assertThat(ran).containsExactly("first", "second", "third");
            }

            @Test
            void shouldSuppressExceptionsAndContinueRunningRemainingRunnables() {
                var ran = new ArrayList<String>();
                assertThatCode(() -> Utils.silentlyRun(
                        () -> ran.add("first"),
                        () -> { throw new RuntimeException("oops"); },
                        () -> ran.add("third")
                )).doesNotThrowAnyException();
                assertThat(ran).containsExactly("first", "third");
            }
        }
    }

    @Nested
    class SafelyClose {

        @Test
        void shouldCloseResources() throws Exception {
            var resource1 = new TrackingCloseable();
            var resource2 = new TrackingCloseable();

            assertThatCode(() -> Utils.safelyClose(resource1, resource2)).doesNotThrowAnyException();

            assertThat(resource1.closed).isTrue();
            assertThat(resource2.closed).isTrue();
        }

        @Test
        void shouldSkipNullInstances() {
            assertThatCode(() -> Utils.safelyClose((Object) null)).doesNotThrowAnyException();
        }

        @Test
        void shouldSuppressExceptionsThrownWhileClosing() {
            var exploding = new ExplodingCloseable();
            assertThatCode(() -> Utils.safelyClose(exploding)).doesNotThrowAnyException();
        }

        @Test
        void shouldUseCustomMethodNameFromPair() {
            var resource = new TrackingCloseable();
            assertThatCode(() -> Utils.safelyClose(Pair.of(resource, "customClose"))).doesNotThrowAnyException();
            assertThat(resource.customCloseCalled).isTrue();
            assertThat(resource.closed).isFalse();
        }

        static class TrackingCloseable {
            boolean closed = false;
            boolean customCloseCalled = false;

            public void close() {
                closed = true;
            }

            public void customClose() {
                customCloseCalled = true;
            }
        }

        static class ExplodingCloseable {
            public void close() {
                throw new RuntimeException("boom");
            }
        }
    }

    @Nested
    class FunctionThrowsExceptionInterface {

        @Test
        void shouldApplyFunction() throws Exception {
            Utils.FunctionThrowsException<String, Integer> fn = String::length;
            assertThat(fn.apply("hello")).isEqualTo(5);
        }
    }

    @Nested
    class RunnableThrowsExceptionInterface {

        @Test
        void shouldRunWithoutException() throws Exception {
            List<String> ran = new ArrayList<>();
            Utils.RunnableThrowsException r = () -> ran.add("ran");
            r.run();
            assertThat(ran).containsExactly("ran");
        }
    }
}
