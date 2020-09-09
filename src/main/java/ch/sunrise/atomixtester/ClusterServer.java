package ch.sunrise.atomixtester;

import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.cluster.discovery.NodeDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.core.map.MapEvent;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.profile.Profile;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClusterServer implements MapEventListener<String, String> {


    private ClusterConfiguration config;
    private String memberId;
    private ILogger logger;
    private Atomix atomix;
    private DistributedMap<String, String> map;
    private String mapName = "my-map";

    public ClusterServer(ClusterConfiguration config, String memberId, ILogger logger) {
        this.config = config;
        this.memberId = memberId;
        this.logger = logger;
    }

    public void initialize() {
        var builder = Atomix.builder();

        var myClusterAddress = config.getMembers().stream()
                .filter(u -> u.getMemberId().equals(memberId)).findFirst().orElse(null);
        if (myClusterAddress == null)
        {
            log("Unable to find member " + memberId + " in cluster configuration", 1);
            return;
        }
        var otherAddresses = config.getMembers().stream().filter(u -> !u.getMemberId().equals(memberId));
        var myAddress = new Address(myClusterAddress.getIpOrHost(), myClusterAddress.getPort());
        builder = builder.withMemberId(memberId).withAddress(myAddress);

        List<Node> nodes = new ArrayList<>();
        otherAddresses.forEach(u -> nodes.add(Node.builder().withId(u.getMemberId())
                .withAddress(new Address(u.getIpOrHost(), u.getPort())).build()));

        builder = builder.withMembershipProvider(BootstrapDiscoveryProvider.builder().withNodes(nodes).build());

        builder.addProfile(Profile.dataGrid());

        atomix = builder.build();
    }

    public void start() {
        atomix.start().join();

        DistributedMapBuilder<String, String> mapBuilder = atomix.mapBuilder(mapName);
        map = mapBuilder.withCacheEnabled().build();

        map.addListener(this);

        map.put("Status-" + memberId, "online");

//        MultiRaftProtocol protocol = MultiRaftProtocol.builder()
//                .withReadConsistency(ReadConsistency.LINEARIZABLE)
//                .build();
//
//        Map<String, String> map = atomix.<String, String>mapBuilder("my-map")
//                .withProtocol(protocol)
//                .build();
//
//        map = atomix.<String, String>.mapBuilder("my-map")
//                .withCacheEnabled()
//                .build();
//
//        map.put

    }

    public void runTest() {
        var status1 = map.get("Status-member1");
        var status2 = map.get("Status-member2");
        map.put("Status-" + memberId, "running");
//        Map<String, String> myMap = atomix.getMap(mapName);
    }

    public void runTest(int testNumber) {
        map.put("Test-" + memberId, "" + testNumber);
    }

    public void stop() {
        map.removeListener(this);
        map.close();
        atomix.stop().join();
    }

    private void test() {
//        Atomix atomix = Atomix.builder()
//                .withMemberId("member-1")
//                .withAddress("10.192.19.181:5679")
//                .withMembershipProvider(BootstrapDiscoveryProvider.builder()
//                        .withNodes(
//                                Node.builder()
//                                        .withId("member1")
//                                        .withAddress("10.192.19.181:5679")
//                                        .build(),
//                                Node.builder()
//                                        .withId("member2")
//                                        .withAddress("10.192.19.182:5679")
//                                        .build(),
//                                Node.builder()
//                                        .withId("member3")
//                                        .withAddress("10.192.19.183:5679")
//                                        .build())
//                        .build())
//                .withManagementGroup(RaftPartitionGroup.builder("system")
//                        .withNumPartitions(1)
//                        .withMembers("member-1", "member-2", "member-3")
//                        .build())
//                .withPartitionGroups(
//                        PrimaryBackupPartitionGroup.builder("data")
//                                .withNumPartitions(32)
//                                .build())
//                .build();
    }

    private void log(String message, int severity) {
        logger.Log(message, severity);
    }


    @Override
    public void event(MapEvent<String, String> stringStringMapEvent) {
        switch (stringStringMapEvent.type()) {
            case INSERT:
                log(memberId + ":  New value added to map " + stringStringMapEvent.key() + " value " + stringStringMapEvent.newValue(), 4);
                break;
            case UPDATE:
                log(memberId + ": Value updated in map, key: " + stringStringMapEvent.key() + " new value " + stringStringMapEvent.newValue(), 4);
                break;
            case REMOVE:
                break;
        }
    }
}
