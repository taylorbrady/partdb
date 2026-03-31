package io.partdb.app;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JsonOutputTest {

    @Test
    void quoteEscapesSpecialCharacters() {
        assertEquals(
            "\"node\\\"1\\\\line\\n\\t\"",
            JsonWriter.quote("node\"1\\line\n\t")
        );
    }
}
