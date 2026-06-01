package org.kiwiproject.dropwizard.activemq.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertNoPropertyViolations;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertNoViolations;
import static org.kiwiproject.test.validation.ValidationTestHelper.assertOnePropertyViolation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.kiwiproject.validation.KiwiValidations;

@DisplayName("DestinationNormalizerConfig")
class DestinationNormalizerConfigTest {

    private DestinationNormalizerConfig config;

    @BeforeEach
    void setUp() {
        config = new DestinationNormalizerConfig();
    }

    @Nested
    class DefaultValues {

        @Test
        void shouldHaveExpectedDefaults() {
            assertAll(
                    () -> assertThat(config.getPattern()).isNull(),
                    () -> assertThat(config.getReplacement()).isEmpty()
            );
        }
    }

    @Nested
    class Builder {

        @Test
        void shouldBuildWithDefaults() {
            var built = DestinationNormalizerConfig.builder().build();
            var defaulted = new DestinationNormalizerConfig();

            assertAll(
                    () -> assertThat(built.getPattern()).isNull(),
                    () -> assertThat(built.getReplacement()).isEqualTo(defaulted.getReplacement())
            );
        }

        @Test
        void shouldBuildWithExplicitValues() {
            var built = DestinationNormalizerConfig.builder()
                    .pattern("(myapp\\.user).*")
                    .replacement("$1.##")
                    .build();

            assertAll(
                    () -> assertThat(built.getPattern()).isEqualTo("(myapp\\.user).*"),
                    () -> assertThat(built.getReplacement()).isEqualTo("$1.##")
            );
        }

        @Test
        void shouldDefaultReplacementToEmpty_WhenNullIsGiven() {
            var built = DestinationNormalizerConfig.builder()
                    .pattern("(myapp\\.group).*")
                    .replacement(null)
                    .build();

            assertThat(built.getReplacement()).isEmpty();
        }

        @Test
        void shouldPassBeanValidation_WhenPatternIsValid() {
            var built = DestinationNormalizerConfig.builder()
                    .pattern("(myapp\\.group).*")
                    .replacement("$1.##")
                    .build();

            assertNoViolations(built);
        }
    }

    @Nested
    class Validation {

        @Test
        void shouldPassValidation_WhenAllRequiredFieldsArePresent() {
            config.setPattern("(myapp\\.group).*");

            assertNoViolations(config);
        }

        @Nested
        class Pattern {

            @ParameterizedTest
            @NullAndEmptySource
            @ValueSource(strings = {"  ", "\t"})
            void shouldFailValidation_WhenBlankOrNull(String value) {
                config.setPattern(value);

                assertOnePropertyViolation(config, "pattern");
            }

            @Test
            void shouldFailValidation_WhenInvalidRegex() {
                config.setPattern("[invalid");

                var violations = KiwiValidations.validate(config);
                assertThat(violations)
                        .extracting(v -> v.getPropertyPath().toString())
                        .contains("patternValid");
            }

            @Test
            void shouldPassValidation_WhenValidRegex() {
                config.setPattern("(myapp\\.group).*");

                assertNoPropertyViolations(config, "pattern");
            }
        }

        @Nested
        class Replacement {

            @Test
            void shouldFailValidation_WhenNull() {
                config.setReplacement(null);

                assertOnePropertyViolation(config, "replacement");
            }

            @Test
            void shouldPassValidation_WhenEmpty() {
                assertNoPropertyViolations(config, "replacement");
            }

            @Test
            void shouldPassValidation_WhenNonEmpty() {
                config.setReplacement("$1.##");

                assertNoPropertyViolations(config, "replacement");
            }
        }
    }
}
