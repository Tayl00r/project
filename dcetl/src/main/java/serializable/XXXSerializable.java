package serializable;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class XXXSerializable {

	public XXXSerializable() {
		// TODO Auto-generated constructor stub
	}
	
	private void writeObject(ObjectOutputStream out, Object obj) throws IOException, ClassNotFoundException {
        out.writeObject(obj);
        out.close();
    }

    private Object readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        Object obj = in.readObject();
        in.close();
        return obj;
    }

}
