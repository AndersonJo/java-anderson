package ai.incredible;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.*;

class CalculatorTest {
    Calculator calculator;

    /**
     * 매번 테스트 함수가 실행될때마다 실행됨
     */
    @BeforeEach
    void setUp() { //
        calculator = new Calculator();
    }

    @Test
    @DisplayName("Simple Summation")
    void simpleSum() {
        assertEquals(20, calculator.sum(10, 10), "simple summation");
    }

    @RepeatedTest(5)
    @DisplayName("Ensure correct handling of 0")
    void multipleSum() {
        assertEquals(0, calculator.sum(5, -5), "it should be 0");
    }

    @Test
    @DisplayName("Grouped Assertions")
    void groupedAssertions() {
        assertAll("Everything should work well",
                () -> assertEquals(10, calculator.sum(5, 5)),
                () -> assertEquals(20, calculator.sum(0, 20))
        );
    }

    @Test
    @DisplayName("HTTP Connection Test")
    void timeoutNotExceeded() throws IOException {
        URL url = new URL("https://raw.githubusercontent.com/zauberware/personal-interests-json/master/data/interests.json");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Content-Type", "text/html");
        connection.setUseCaches(false);
        connection.setDoOutput(true);

        final StringBuilder stringBuilder = new StringBuilder();
        assertTimeout(ofSeconds(1),
                () -> {
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                    String line;
                    while ((line = bufferedReader.readLine()) != null) {
                        stringBuilder.append(line);
                        stringBuilder.append('\r');
                    }
                    bufferedReader.close();
                });
        String response = stringBuilder.toString();
        assertEquals(200, connection.getResponseCode());
        assertTrue(10 < response.length());
    }
}