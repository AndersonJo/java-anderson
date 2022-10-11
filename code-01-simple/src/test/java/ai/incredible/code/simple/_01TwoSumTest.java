package ai.incredible.code.simple;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class _01TwoSumTest {

    _01TwoSum model;

    @BeforeEach
    void setUp() {
        model = new _01TwoSum();
    }

    @AfterEach
    void tearDown() {
        model = null;
    }


    @Test
    void test01(){
        int[] nums = {1, 2, 3, 4};
        int[] ans = {0, 1};
        assertArrayEquals(ans, model.twoSum(nums, 3));
    }

    @Test
    void test02(){
        int[] nums = {1, 2, 3, 4};
        int[] ans = {0, 1};
        assertNull(model.twoSum(nums, 1000));
    }
}