#MetaQ PHP Client library

* Support producer short connection and long connection, this require config file
* Producer random select a broker-partition for load balance, no proxy required
* Support master async salve group cluster, circle backups
* Producer auto failover
* Consumer will read from both master and slaves
* Slave broker can be read by consumers
* Store consumer offset in zookeeper
* Consumer do not require the config file, they will fetch meta data from zookeeper
* Consumer will auto-rebalance if consumer group changes
* Suggest to use superviord auto restarting when consumer process dies. see http://supervisord.org/


##MetaQ server installation and configurations:

    wget http://fnil.net/downloads/metaq-server-1.4.6.2.tar.gz

    Web UI of MetaQ: http://192.168.1.149:8120/
    Web UI of zookeeper: https://github.com/qiuxiafei/zk-web.git

##Notes

1. Remember to set updateConsumerOffsets=false

2. Master node configuration:


    [system]
    brokerId=0
    numPartitions=1
    serverPort=8123
    dashboardHttpPort=8120
    unflushThreshold=1000
    unflushInterval=10000
    maxSegmentSize=1073741824
    maxTransferSize=1048576
    deletePolicy=delete,168
    deleteWhen=0 0 6,18 * * ?
    flushTxLogAtCommit=1
    stat=true
    getProcessThreadCount=40
    putProcessThreadCount=40
    dataPath=/home/bruce/meta2
    
    ;; Update consumers offsets to current max offsets when consumers offsets are out of range of current broker's messages.
    ;; It must be false in production.But recommend to be true in development or test.
    updateConsumerOffsets=false

    [zookeeper]
    zk.zkConnect=localhost:2181
    zk.zkSessionTimeoutMs=30000
    zk.zkConnectionTimeoutMs=30000
    zk.zkSyncTimeMs=5000

    ;; Topics section
    [topic=t1]
    numPartitions=5

3. Slave configuration:

    slaveId=1
    slaveGroup=meta-slave-group
    slaveMaxDelayInMills=50
    autoSyncMasterConfig=true

4. Install PHP zookeeper extension:

    4.1. Install libzookeeper
    4.2. pecl install zookeeper-0.2.2
