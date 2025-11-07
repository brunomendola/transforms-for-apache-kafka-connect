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

import java.util.Map;

import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FilterByHeaderValueTest {
    @Test
    void shouldFilterOutRecordWithMatchingHeaderValue() {
        final FilterByHeaderValue<SourceRecord> filter = new FilterByHeaderValue<>();
        filter.configure(Map.of(
            "header.name", "eventType",
            "header.value", "delete",
            "header.value.matches", true
        ));
        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, null);
        Headers headers = new ConnectHeaders();
        headers.add("eventType", "delete", null);
        record.headers().add("eventType", "delete", null);
        assertThat(filter.apply(record)).isEqualTo(record);
    }

    @Test
    void shouldFilterOutRecordWithNonMatchingHeaderValue() {
        final FilterByHeaderValue<SourceRecord> filter = new FilterByHeaderValue<>();
        filter.configure(Map.of(
            "header.name", "eventType",
            "header.value", "delete",
            "header.value.matches", true
        ));
        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, null);
        record.headers().add("eventType", "update", null);
        assertThat(filter.apply(record)).isNull();
    }

    @Test
    void shouldFilterOutRecordWithMatchingHeaderValueMatchesFalse() {
        final FilterByHeaderValue<SourceRecord> filter = new FilterByHeaderValue<>();
        filter.configure(Map.of(
            "header.name", "eventType",
            "header.value", "delete",
            "header.value.matches", false
        ));
        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, null);
        record.headers().add("eventType", "delete", null);
        assertThat(filter.apply(record)).isNull();
    }

    @Test
    void shouldNotFilterOutRecordWithNonMatchingHeaderValueMatchesFalse() {
        final FilterByHeaderValue<SourceRecord> filter = new FilterByHeaderValue<>();
        filter.configure(Map.of(
            "header.name", "eventType",
            "header.value", "delete",
            "header.value.matches", false
        ));
        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, null);
        record.headers().add("eventType", "update", null);
        assertThat(filter.apply(record)).isEqualTo(record);
    }
}
