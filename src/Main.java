import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * User: trevor hodde
 * Date: 9/25/13
 * Time: 11:00 PM
 */
public class Main {
    public static void main (String[] args) {
        final File MY_PAGE_FILE = new File("my_page.csv");
        final File FRIEND_FILE = new File("friend_page.csv");
        final File ACCESS_LOG_FILE = new File("access_log.csv");

        Scanner sc = new Scanner(System.in);
        String generate = "";

        // If any of the files do not exist, we need to generate them all
        if (!MY_PAGE_FILE.exists() || !FRIEND_FILE.exists() || !ACCESS_LOG_FILE.exists()) {
            System.out.println("One or more datasets do not exist. Creating them...");
            generate = "y";
        }
        else {
            // If the datasets already exist, ask the user if they would like to regenerate them since it
            // takes so long to create new data sets each time
            System.out.println("Datasets already exist. Would you like to regenerate them? <y/n> ");
            generate = sc.nextLine();
        }

        // Run the data set generator ** WARNING: This takes between 10 to 30 minutes to run **
        if (generate.equalsIgnoreCase("y") || generate.equalsIgnoreCase("yes") || generate.equals("") || generate.equals(null)) {
            DataSetGenerator dsg = new DataSetGenerator();
            dsg.myPageGenerator();
            dsg.friendsGenerator();
            dsg.accessLogGenerator();
        }

        TrevorTasks t1 = new TrevorTasks();

        try {
            t1.doTaskB(MY_PAGE_FILE);
        }
        catch(Exception e) {
            System.out.println(e);
        }
    }
}
