package manning.bigdata.ch3;

import com.backtype.hadoop.pail.PailStructure;

import java.io.*;
import java.util.Collections;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 02.03.14
 * Time: 12:02
 * To change this template use File | Settings | File Templates.
 */
public class LoginPailStructure implements PailStructure<Login> {

    @Override
    public boolean isValidTarget(String... strings) {
        return true;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Login deserialize(byte[] bytes) {
        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            byte[] userBytes = new byte[dataIn.readInt()];
            dataIn.read(userBytes);
            return new Login(new String(userBytes), dataIn.readLong());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] serialize(Login login) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        byte[] userBytes = login.userName.getBytes();
        try {
            dataOut.writeInt(userBytes.length);
            dataOut.write(userBytes);
            dataOut.writeLong(login.loginUnixTime);
            dataOut.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOut.toByteArray();
    }

    @Override
    public List<String> getTarget(Login login) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Class getType() {
        return Login.class;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
