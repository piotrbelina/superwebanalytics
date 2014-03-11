package manning.bigdata.ch3;

/**
 * Created with IntelliJ IDEA.
 * User: bela
 * Date: 02.03.14
 * Time: 12:01
 * To change this template use File | Settings | File Templates.
 */
public class Login {
    public String userName;
    public long loginUnixTime;

    public Login(String _user, long _login) {
        userName = _user;
        loginUnixTime = _login;
    }
}
