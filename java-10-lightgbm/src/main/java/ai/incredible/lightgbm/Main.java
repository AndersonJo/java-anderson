package ai.incredible.lightgbm;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.microsoft.ml.lightgbm.PredictionType;
import io.github.metarank.lightgbm4j.LGBMBooster;

import java.io.File;
import java.util.Arrays;

public class Main {
	static private final String MODEL_PATH = "/home/anderson/Desktop/model.txt";
	static private final String DATA_PARQUET_DIR = "/tmp/lightgbm4j/";

	@lombok.SneakyThrows
	public static void main(String[] args) {
		File file = new File(MODEL_PATH);
		String modelContent;
		modelContent = Files.toString(file, Charsets.UTF_8);
		LGBMBooster model = LGBMBooster.loadModelFromString(modelContent);
		System.out.println(model);

		float[] input =
			new float[] { 0.700720f, 1.287160f, -2.085664f, -0.004941f, 0.249742f, -0.323739f,
				-1.946551f, 1.496363f };

		double[] pred = model.predictForMat(input, 1, 8, true,
			PredictionType.C_API_PREDICT_NORMAL);
		System.out.println(Arrays.toString(pred));
	}
}