/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class FilterByHeaderValue<R extends ConnectRecord<R>> implements Transformation<R> {

  private String headerName;
  private Optional<String> headerExpectedValue;
  private Optional<String> headerValuePattern;

  @Override
  public ConfigDef config() {
    return new ConfigDef()
        .define("header.name",
            ConfigDef.Type.STRING,
            "key",
            ConfigDef.Importance.HIGH,
            "The header name to filter by."
            + "If empty, the message key header will be filtered.")
        .define("header.value",
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "Expected value to match. Either define this, or a regex pattern")
        .define("header.value.pattern",
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            "The pattern to match. Either define this, or an expected value")
        .define("header.value.matches",
            ConfigDef.Type.BOOLEAN,
            true,
            ConfigDef.Importance.MEDIUM,
            "The filter mode, 'true' for matching or 'false' for non-matching");
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    final AbstractConfig config = new AbstractConfig(config(), configs);
    this.headerName = Optional.ofNullable(config.getString("header.name")).orElse("key");
    this.headerExpectedValue = Optional.ofNullable(config.getString("header.value"));
    this.headerValuePattern = Optional.ofNullable(config.getString("header.value.pattern"));
    final boolean expectedValuePresent = headerExpectedValue.isPresent();
    final boolean regexPatternPresent = headerValuePattern.map(s -> !s.isEmpty()).orElse(false);
    if (expectedValuePresent == regexPatternPresent) {
      throw new ConfigException(
          "Either header.value or header.value.pattern have to be set to apply filter transform");
    }
    final Predicate<String> matchCondition;

    if (expectedValuePresent) {
      final String expectedValue = headerExpectedValue.get();
      matchCondition = expectedValue::equals;
    } else {
      final String pattern = headerValuePattern.get();
      final Predicate<String> regexPredicate = Pattern.compile(pattern).asPredicate();
      matchCondition = str ->
          str != null && regexPredicate.test(str);
    }

    this.filterCondition = config.getBoolean("header.value.matches")
        ? matchCondition
        : (result -> !matchCondition.test(result));
  }

  private Predicate<String> filterCondition;

  @Override
  public R apply(final R record) {
    return filterCondition.test(record.headers().lastWithName(headerName).value().toString()) ? record : null;
  }

  @Override
  public void close() {
  }
}
