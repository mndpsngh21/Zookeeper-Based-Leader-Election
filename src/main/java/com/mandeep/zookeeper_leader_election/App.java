package com.mandeep.zookeeper_leader_election;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

public class App {
	public static void main( String[] args ) throws IOException, InterruptedException, KeeperException
    {
        LeaderElection leaderElection  = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.run();
        leaderElection.close();

    }
}
