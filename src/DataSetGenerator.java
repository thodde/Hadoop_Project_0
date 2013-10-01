/**
 * User: trevor hodde
 * Date: 9/25/13
 * Time: 10:14 PM
 */
import java.util.Random;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.File;

public class DataSetGenerator {
    // File names for storing the data sets
    final private String MY_PAGE_FILE = "my_page.csv";
    final private String FRIEND_FILE = "friend_page.csv";
    final private String ACCESS_LOG_FILE = "access_log.csv";

    final private String ALPHABET = "abcdefghijklmnopqrstuvwxyz";

    // Max number of records to store in the data sets
    final private int MY_PAGE_RECORDS = 50000;
    final private int FRIENDS_RECORDS = 5000000;
    final private int ACCESS_RECORDS = 10000000;

    // Just a more interesting way to store Strings for descriptions later on
    public enum Description {
        High_School_Friends("High School Friends"), College_Friends("College Friends"),
        Unknown("Unknown"), Family("Family"), Colleague("Colleague");
        private String value;

        Description (String value) {
            this.value = value;
        }

        // Generate a random description
        private static final List<Description> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static String randomDescription()  {
            return VALUES.get(RANDOM.nextInt(SIZE)).toString();
        }
    }

    // Makes reading hobbies more interesting
    public enum Hobby {
        Baseball("Baseball"), Basketball("Basketball"), Programming("Programming"),
        Music("Music"), Running("Running"), Movies("Movies");
        private String value;

        Hobby (String value) {
            this.value = value;
        }

        // Generate a random hobby
        private static final List<Hobby> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static String randomHobby()  {
            return VALUES.get(RANDOM.nextInt(SIZE)).toString();
        }
    }

    // A better way to store Strings for Nationalities later on
    public enum Nationality {
        German("German"), French("French"), Chinese("Chinese"), American("American"), Indian("Indian"),
        Italian("Italian"), English("English"), Japanese("Japanese");
        private String value;

        Nationality (String value) {
            this.value = value;
        }

        // Generate a random nationality
        private static final List<Nationality> VALUES = Collections.unmodifiableList(Arrays.asList(values()));
        private static final int SIZE = VALUES.size();
        private static final Random RANDOM = new Random();

        public static String randomNationality()  {
            return VALUES.get(RANDOM.nextInt(SIZE)).toString();
        }

        // Assign a particular country code based on the position of the nationality in the array
        public static int getCountryCode(String strNationality) {
            int index = 0;

            for(Nationality n : VALUES) {
                if (n.toString().equals(strNationality)) {
                    return index;
                }
                else {
                    index++;
                }
            }
            return index;
        }
    }

    /**
     * Generate the my_page.csv file
     */
    public void myPageGenerator() {
        int currentID;
        String currentName;
        String currentNationality;
        int currentCountryCode;
        String currentHobby;

        Random r = new Random();
        File f = new File(MY_PAGE_FILE);

        // Remove the MyPage file if it already exists
        if(f.exists()) {
            System.out.println("MyPage dataset already exists! Deleting...");
            f.delete();
        }

        System.out.println("Generating MyPage dataset...");

        // Generate random data to store in the data set
        for(int i = 1; i <= MY_PAGE_RECORDS; i++) {
            currentID = i;
            currentName = generateRandomString(r.nextInt(10) + 10);

            // Generate random nationality using the enum above
            currentNationality = Nationality.randomNationality();

            // Generate random hobby using the enum above
            currentHobby = Hobby.randomHobby();

            currentCountryCode = Nationality.getCountryCode(currentNationality);

            try {
                // Append the new data to the file
                PrintWriter out = new PrintWriter(new FileWriter(MY_PAGE_FILE, true));
                // Create a comma separated string to write out
                out.println(currentID + "," + currentName + "," + currentNationality + "," + currentCountryCode + ","
                        + currentHobby);
                out.close();
            }
            catch(IOException e) {
                System.out.println("Error writing to MyPage file!");
            }
        }
    }

    /**
     * Generate the friends.csv file
     */
    public void friendsGenerator() {
        int currentFriendRel;
        int currentPersonID;
        int currentFriendID;
        int currentDateOfFriendship;
        String currentDesc = "";

        Random r = new Random();
        File f = new File(FRIEND_FILE);

        if(f.exists()) {
            System.out.println("Friend dataset already exists! Deleting...");
            f.delete();
        }

        System.out.println("Generating Friend dataset...");

        // Fill the data set with random data
        for(int i = 1; i <= FRIENDS_RECORDS; i++) {
            currentFriendRel = i;
            currentPersonID = r.nextInt(MY_PAGE_RECORDS) + 1;
            currentFriendID = r.nextInt(MY_PAGE_RECORDS) + 1;
            currentDateOfFriendship = r.nextInt(1000000) + 1;

            // Generate random descriptions using the enum above
            currentDesc = Description.randomDescription();

            try {
                // Append the new data to the data set
                PrintWriter out = new PrintWriter(new FileWriter(FRIEND_FILE, true));
                // Write out a comma separated string to the file
                out.println(currentFriendRel + "," + currentPersonID + "," + currentFriendID + "," +
                        currentDateOfFriendship + "," + currentDesc);
                out.close();
            }
            catch(IOException e) {
                System.out.println("Error writing to Friend file!");
            }
        }
    }

    /**
     * Generate the accesslog.csv file
     */
    public void accessLogGenerator() {
        int currentAccessID;
        int currentByWho;
        int currentWhatPage;
        String currentType;
        int currentAccessTime;

        Random r = new Random();
        File f = new File(ACCESS_LOG_FILE);

        // Check if the AccessLog file already exists, if so, delete it
        if(f.exists()) {
            System.out.println("AccessLog dataset already exists! Deleting...");
            f.delete();
        }

        System.out.println("Generating AccessLog dataset...");

        for (int i = 1; i <= ACCESS_RECORDS; i++) {
            //generate a bunch of random data for the access log
            currentAccessID = i;
            currentByWho = r.nextInt(50000) + 1;
            currentWhatPage = r.nextInt(50000) + 1;
            currentType = generateRandomString(r.nextInt(50 - 20) + 20 + 1);
            currentAccessTime = r.nextInt(1000000);

            try {
                // Append the new data to the data set
                PrintWriter out = new PrintWriter(new FileWriter(ACCESS_LOG_FILE, true));
                // Write out a comma separated string to the file
                out.println(currentAccessID + "," + currentByWho + "," + currentWhatPage + "," +
                        currentType + "," + currentAccessTime);
                out.close();
            }
            catch(IOException e) {
                System.out.println("Error writing to Friend file!");
            }
        }
    }

    /**
     * This method generates a random string of characters
     *
     * @param randomStringLength The number of characters to generate
     * @return a random string of characters
     */
    private String generateRandomString (int randomStringLength) {
        StringBuffer randStr = new StringBuffer();

        for(int i = 0; i < randomStringLength; i++) {
            int number = getRandomNumber();
            char ch = ALPHABET.charAt(number);
            randStr.append(ch);
        }
        return randStr.toString();
    }

    /**
     * This method generates random numbers
     * @return int
     */
    private int getRandomNumber() {
        int randomInt = 0;
        Random randomGenerator = new Random();
        randomInt = randomGenerator.nextInt(ALPHABET.length());
        if (randomInt - 1 == -1) {
            return randomInt;
        }
        else {
            return randomInt - 1;
        }
    }
}