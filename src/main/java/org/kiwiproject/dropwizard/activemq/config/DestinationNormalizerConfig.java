package org.kiwiproject.dropwizard.activemq.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

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
 * <p>
 * Example — normalize a user-group destination to a wildcard form:
 * <pre>
 *   pattern:     "(myapp.group).*"
 *   replacement: "$1.##"
 * </pre>
 */
@Getter
@Setter
public class DestinationNormalizerConfig {

    @NotBlank
    private String pattern;

    @NotNull
    private String replacement = "";
}
