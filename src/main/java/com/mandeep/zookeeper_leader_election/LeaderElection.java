package com.mandeep.zookeeper_leader_election;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class LeaderElection
{
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/sampleznode";
    private String currentZNodeName;
	final CountDownLatch connectedSignal = new CountDownLatch(1);

	ZookeeperWatcher zookeeperWatcher;

	public LeaderElection(){
		zookeeperWatcher= new ZookeeperWatcher(this);
	}
	public void volunteerForLeadership(){
		try {
			connectedSignal.await();
		Stat stat = zooKeeper.exists(ELECTION_NAMESPACE,false);
		if(stat==null){
			// create znode
			zooKeeper.create(ELECTION_NAMESPACE,  new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		// create node path
		String zNodePrefix = ELECTION_NAMESPACE + "/id_";
		String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[] {}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println("znode_name: " + zNodeFullPath);
		this.currentZNodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
		// trigger election
			reelectLeader();
		} catch (InterruptedException | KeeperException e) {
			throw new RuntimeException(e);
		}

	}
	
	public void reelectLeader() throws KeeperException, InterruptedException {
		Stat predecessorStat = null;
		String predecessorZnodeName = "";
		boolean isLeader=false;
		while (predecessorStat == null) {
			List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
			Collections.sort(children);
			
			String smallestChild = children.get(0);
			
			if (smallestChild.equals(currentZNodeName)) {
				isLeader=true;
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + smallestChild, zookeeperWatcher);
			} else {
				System.out.println("I am not the leader ");
				int predecessorIndex = Collections.binarySearch(children, currentZNodeName) - 1;
				predecessorZnodeName = children.get(predecessorIndex);
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZnodeName, zookeeperWatcher);
			}
		}

		if(isLeader){
			System.out.println("I am the leader !!");
		}
		
		System.out.println("Watching node: " + predecessorZnodeName);
		System.out.println();
	}
	
	public void watchTargetZNode() throws KeeperException, InterruptedException {
		Stat stat = zooKeeper.exists(ELECTION_NAMESPACE, zookeeperWatcher);
		if (stat == null) {
			return;
		}
		
		byte[] data = zooKeeper.getData(ELECTION_NAMESPACE, zookeeperWatcher, stat);
		List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, zookeeperWatcher);
		
		System.out.println("Data: " + new String(data) + " Children: " + children);
	}
	
	public ZooKeeper connectToZookeeper() throws IOException{
		this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, zookeeperWatcher);
		return zooKeeper;

	}
	
	public void run() throws InterruptedException {
		synchronized (zooKeeper) {
			zooKeeper.wait();
		}
	}
	
	public void close() throws InterruptedException {
		zooKeeper.close();
	}

	public void updateCountDown() {
		connectedSignal.countDown();
	}

	public void onDisconnection() {
		synchronized (zooKeeper) {
			System.out.println("Disconnected from zookeeper event");
			zooKeeper.notifyAll();
		}
	}

	public void onNodeCreated() {
		System.out.println("Node "+ELECTION_NAMESPACE + " got created!!");
	}

	public void onNodeDataChanged() {
		System.out.println(ELECTION_NAMESPACE + " data changed!!");
	}

	public void onChildrenInfoChanged() {
		System.out.println(ELECTION_NAMESPACE + " children changed!!");
		try {
			watchTargetZNode();
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
