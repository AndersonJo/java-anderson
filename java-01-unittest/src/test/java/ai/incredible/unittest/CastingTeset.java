package ai.incredible.unittest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class CastingTeset {

    @Test
    void testLong(){
        Long value = 13L;
        value = Long.parseLong(String.valueOf(value));
        assertEquals(13L, value);

    }

}
