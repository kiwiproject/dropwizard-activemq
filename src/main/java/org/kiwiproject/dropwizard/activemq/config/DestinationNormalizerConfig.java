package org.kiwiproject.dropwizard.activemq.config;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNullElse;

import io.dropwizard.validation.ValidationMethod;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Configures a single destination normalizer: a regex {@code pattern} applied to a destination
 * string and a {@code replacement} used in its place.
 * <p>
 * Normalizers are applied only when simplifying destination names for
 * <a href="https://github.com/elucidation-project/elucidation">Elucidation</a> event
 * recording. They have no effect on actual JMS routing.
 * <p>
 * Some applications route messages to destinations that contain dynamic identifiers — for example,
 * a separate topic per user or per group. Elucidation tracks service dependencies by destination
 * name, so without normalization each unique identifier would appear as a distinct connection,
 * obscuring the underlying relationship. A normalizer collapses those destinations into a canonical
 * form (e.g., {@code myapp.user.##}) so Elucidation sees them as a single logical connection type.
 * <p>
 * The pattern and replacement follow {@link java.util.regex.Matcher#replaceAll(String)} semantics,
 * so capturing groups ({@code $1}, {@code $2}, etc.) are supported in the replacement string.
 * The pattern must be a valid Java regex; an invalid pattern is caught during startup validation.
 * <p>
 * Example — normalize a user-group destination to a wildcard form:
 * <pre>
 *   pattern:     "(myapp.group).*"
 *   replacement: "$1.##"
 * </pre>
 */
@Getter
@Setter
@NoArgsConstructor
public class DestinationNormalizerConfig {

    @NotBlank
    private String pattern;

    @NotNull
    private String replacement = "";

    @ValidationMethod(message = "pattern must be a valid regular expression")
    public boolean isPatternValid() {
        if (isNull(pattern)) {
            return true;
        }
        try {
            Pattern.compile(pattern);
            return true;
        } catch (PatternSyntaxException e) {
            return false;
        }
    }

    /**
     * Builder constructor for programmatic construction.
     * <p>
     * Note: {@code pattern} has no default and must be supplied;
     * it is required by the {@code @NotBlank} constraint.
     */
    @Builder
    private DestinationNormalizerConfig(String pattern, String replacement) {
        this.pattern = pattern;
        this.replacement = requireNonNullElse(replacement, "");
    }
}
