package ai.incredible.gson;

import com.google.gson.Gson;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GsonTest {
    Gson gson;

    public GsonTest() {
        gson = new Gson();
    }

    InputStreamReader getAsStream(String filepath) {
        return new InputStreamReader(Objects.requireNonNull(getClass().getClassLoader().getResourceAsStream(filepath)));
    }

    @Test
    public void testCar(){
        Reader reader = getAsStream("car.json");
        Car car = gson.fromJson(reader, Car.class);
        System.out.println(car);

        assertEquals(car.brand, "Samsung");
        assertEquals(car.power, 123);
        assertEquals(car.missing, false);  // 기본적으로 false 이다
        assertEquals(car.missingTrue, true); // default=true 이면 true 로 나온다
        assertEquals(car.missingFalse, false); // default=false 이면 false 로 나온다
    }

}