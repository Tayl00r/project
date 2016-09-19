package utility;

import com.fasterxml.uuid.Generators;

public class GeneratorUUID {
	
	public static String generateUUID() {
		return Generators.timeBasedGenerator().generate().toString().replace("-", "");
	}

}
