
import org.apache.storm.generated.*;
import topology.FivePlaceLast3HoursTopo;

import topology.NoAvailableBikeStandTopology;
import topology.NoBicycleTopology;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        if (args.length < 1) {

            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
        }

        else if(args.length <= 2) {
            switch (args[0]) {
                case "producer":
                    Producer.main(args);
                    break;
                case "consumer":
                    Consumer.main(args);
                    break;
                default:
                    throw new IllegalArgumentException("Don't know how to do " + args[0]);
            }
        }
        else if (args.length == 4) {
            NoBicycleTopology.main(args);
            NoAvailableBikeStandTopology.main(args);
            FivePlaceLast3HoursTopo.main(args);
        }
        else if (args.length == 5) {
            if (args[4].equals("1")) {
                NoBicycleTopology.main(args);
            }
            else if (args[4].equals("2")) {
                NoAvailableBikeStandTopology.main(args);
            }
            else if (args[4].equals("3")) {
                FivePlaceLast3HoursTopo.main(args);
            }
        }
    }
}
