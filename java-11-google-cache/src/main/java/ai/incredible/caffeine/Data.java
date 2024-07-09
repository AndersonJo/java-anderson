package ai.incredible.caffeine;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Builder
public class Data implements Serializable {

	@Getter
	private int money;

	@Getter
	private String name;
}
