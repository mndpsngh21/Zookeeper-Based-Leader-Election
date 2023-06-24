package com.mandeep.zookeeper_leader_election;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import static org.apache.zookeeper.Watcher.Event.EventType.NodeDeleted;
import static org.apache.zookeeper.Watcher.Event.EventType.None;

public class ZookeeperWatcher implements Watcher {

    LeaderElection leaderElection;

    public ZookeeperWatcher(LeaderElection leaderElection){
        this.leaderElection=leaderElection;
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        switch(watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to zookeeper!!");
                    leaderElection.updateCountDown();
                    leaderElection.volunteerForLeadership();

                } else {
                   leaderElection.onDisconnection();
                }
                break;
            case NodeDeleted:
                try {
                    leaderElection.reelectLeader();
                } catch(KeeperException e) {
                } catch(InterruptedException e) {

                }
            case NodeCreated:
                 leaderElection.onNodeCreated();
            case NodeDataChanged:
                 leaderElection.onNodeDataChanged();
            case NodeChildrenChanged:
                leaderElection.onChildrenInfoChanged();
            default:
                break;
        }
    }
}
