# zookeeper-based-leader-election
A sample code to showcase leader election mechanism and

For this we need a running zookeeper and a znode created on it. you need to use zookeeper setup, for simplicity 
I am using zookeeper docker setup

Start Zookeeper  with below command
``
docker run --name some-zookeeper --restart always -d zookeeper -p 2181:2181
``

make a code build and run as multiple instances, shutdown one node and check behaviour# distributed-system-implementation
