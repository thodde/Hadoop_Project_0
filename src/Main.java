import javax.swing.*;

/**
 * Created with IntelliJ IDEA.
 * User: trevorhodde
 * Date: 9/25/13
 * Time: 11:00 PM
 * To change this template use File | Settings | File Templates.
 */
public class Main {
    public static void main (String[] args) {
        String generate = JOptionPane.showInputDialog(null, "Would you like to regenerate the datasets? <y/n>");

        if (generate.equalsIgnoreCase("y") || generate.equalsIgnoreCase("yes") || generate.equals("") || generate.equals(null)) {
            DataSetGenerator dsg = new DataSetGenerator();
            dsg.myPageGenerator();
            dsg.friendsGenerator();
            dsg.accessLogGenerator();
        }
    }
}
