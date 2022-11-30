package ai.incredible.protobuf;

import com.google.protobuf.ProtocolStringList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class MyProtobufTest {


    public MyProtobufTest() {

    }

    @Test
    public void testMyProtobuf(){
        // Basic Protobuf Test
        Person person = Person.newBuilder()
                .setName("Anderson")
                .setAge(19)
                .addTags("first")
                .addTags("second")
                .setTags(1, "chocolate")
                .build();

        assertEquals(person.getName(), "Anderson");
        assertEquals(person.getAge(), 19);
        System.out.println(person.getTagsList());

        assertEquals(person.getTags(0), "first");
        assertEquals(person.getTags(1), "chocolate");


    }
}