package DataSetGenerator;
/**
 * User: trevor hodde
 * Date: 9/25/13
 * Time: 10:14 PM
 * 
 * editor: Yanqing Zhou
 * Date: 10/5/2013
 * Time: 4:14 PM
 * 
 * 01 decrease size(change back later, l:30)
 * 02 let AccessLog use variable 
 * 03 reset the AccessLog's TypeOfAccess String to an enum
 */
import java.util.Random;
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
    final private int MY_PAGE_RECORDS = 50000;//50000
    final private int FRIENDS_RECORDS = 5000000;//5000000
    final private int ACCESS_RECORDS = 10000000;//10000000

    //just use the String array to save time.
    // Hobbies
    final private String[] Hobby = {"Baseball","Basketball","Programming","Music","Running","Movies"};
    // Nationalities 
    final private String[] Nationality = {"German", "French","Chinese","American","Indian","Italian","English","Japanese"};
    // Friendship descriptions
    final private String[] FriendshipDescription = {"High School Friends","College Friends","Unknown","Family","Colleague"};
    // Type Of Access
    final private String[] TypeOfAccess = {"Just viewed", "Left a note", "Added a friendship"};
    /**
     * Generate the my_page.csv file
     */
    public void myPageGenerator() {
        int currentID;// range: 1 to ACCESS_RECORDS
        String currentName;// range: 10 to 20 random char[].toString
        String currentNationality;// Nationality[currentCountryCode]
        int currentCountryCode;// range: 0 to Nationality.length
        String currentHobby; // Hobby[random]

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
            // Generate name
            currentName = generateRandomString(r.nextInt(10) + 10);//nextInt: range = [0,int)
            // Generate nationality code
            currentCountryCode = r.nextInt(Nationality.length);
            // Use nationality code to get nationality 
            currentNationality = Nationality[currentCountryCode];
            // Generate random hobby
            currentHobby = Hobby[r.nextInt(Hobby.length)];

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
        int currentFriendRel;// range: 1 to FRIENDS_RECORDS
        int currentPersonID;// range: 1 to MY_PAGE_RECORDS
        int currentFriendID;// range: 1 to MY_PAGE_RECORDS
        int currentDateOfFriendship;// range: 1 to 1,000,000 ; indicate when friendship starts
        final int DateOfFriendshipStartRange = 1000000; 
        String currentDesc = "";// FriendshipDescription[random]

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
            currentDateOfFriendship = r.nextInt(DateOfFriendshipStartRange) + 1;
            // Generate Friendship descriptions
            currentDesc = FriendshipDescription[r.nextInt(FriendshipDescription.length)];

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
    	final int AccessTimeLimit = 1000000;
    	
        int currentAccessID;// range: 1 to ACCESS_RECORDS
        int currentByWho;// range: 1 to MY_PAGE_RECORDS
        int currentWhatPage;//range: 1 to MY_PAGE_RECORDS
        String currentTypeOfAccess;// {}
        int currentAccessTime;//range:1 to 1,000,000; the length of this access period, 1 means second
        
        Random r = new Random();//Math.random() simpler to use
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
            currentByWho = r.nextInt(MY_PAGE_RECORDS) + 1;
            currentWhatPage = r.nextInt(MY_PAGE_RECORDS) + 1;
            currentTypeOfAccess = TypeOfAccess[r.nextInt(TypeOfAccess.length)];
            currentAccessTime = r.nextInt(AccessTimeLimit)+1;

            try {
                // Append the new data to the data set
                PrintWriter out = new PrintWriter(new FileWriter(ACCESS_LOG_FILE, true));
                // Write out a comma separated string to the file
                out.println(currentAccessID + "," + currentByWho + "," + currentWhatPage + "," +
                		currentTypeOfAccess + "," + currentAccessTime);
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
        char[] randStr = new char[randomStringLength];
        for(int i = 0; i < randomStringLength; i++) {
            int number = getRandomNumberForALPHABET();
            char ch = ALPHABET.charAt(number);
            randStr[i] = ch;
        }
        return String.valueOf(randStr);
    }

    /**
     * This method generates random numbers
     * @return int
     */
    private int getRandomNumberForALPHABET() {
        int randomInt = 0;
        Random randomGenerator = new Random();
        randomInt = randomGenerator.nextInt(ALPHABET.length());
        if ((randomInt - 1) == -1) {
            return randomInt;
        }
        else {
            return randomInt - 1;
        }
    }
}
