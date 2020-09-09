package ch.sunrise.atomixtester;

import io.atomix.cluster.MemberId;
import io.atomix.cluster.Node;
import io.atomix.cluster.discovery.BootstrapDiscoveryProvider;
import io.atomix.core.Atomix;
import io.atomix.core.election.LeaderElection;
import io.atomix.core.election.LeaderElectionBuilder;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.map.DistributedMap;
import io.atomix.core.map.DistributedMapBuilder;
import io.atomix.core.map.MapEvent;
import io.atomix.core.profile.Profile;
import io.atomix.utils.net.Address;

import java.util.ArrayList;
import java.util.List;

public class ClusterServer {


    private ClusterConfiguration config;
    private String memberId;
    private ILogger logger;
    private Atomix atomix;
    private DistributedMap<String, String> map;
    private String mapName = "my-map", electionName = "my-election";
    private MemberId myMemberId;
    private LeaderElection<String> election;
    private String currentLeader;

    public ClusterServer(ClusterConfiguration config, String memberId, ILogger logger) {
        this.config = config;
        this.memberId = memberId;
        this.logger = logger;
        myMemberId = new MemberId(memberId);
    }

    /**
     * initialize atomix
     */
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
        //var myAddress = new Address(myClusterAddress.getIpOrHost(), myClusterAddress.getPort());
        builder = builder.withMemberId(myMemberId).withHost(myClusterAddress.getIpOrHost()).withPort(myClusterAddress.getPort());

        List<Node> nodes = new ArrayList<>();
        otherAddresses.forEach(u -> nodes.add(Node.builder().withId(u.getMemberId())
                .withAddress(new Address(u.getIpOrHost(), u.getPort())).build()));

        builder = builder.withMembershipProvider(BootstrapDiscoveryProvider.builder().withNodes(nodes).build());

        builder.addProfile(Profile.dataGrid());

        atomix = builder.build();
    }

    /**
     * start atomix, then build a distributed map, register an event handler on it,
     * build and an election builder, register an event on it and run an initial election
     */
    public void start() {
        atomix.start().join();

        DistributedMapBuilder<String, String> mapBuilder = atomix.mapBuilder(mapName);
        map = mapBuilder.withCacheEnabled().build();
        map.addListener(this::event);
        map.put("Status-" + memberId, "online");

        LeaderElectionBuilder<String> electionBuilder = atomix.leaderElectionBuilder(electionName);
        election = electionBuilder.build();
        election.addListener(this::leadershipChanged);
        var myMemberId = atomix.getMembershipService().getLocalMember().id();
        //run initial election
        var electionResult = election.run(myMemberId.id());

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

    /**
     * read from and write to distributed map
     */
    public void runTest() {
        var status1 = map.get("Status-member1");
        var status2 = map.get("Status-member2");
        map.put("Status-" + memberId, "running");
//        Map<String, String> myMap = atomix.getMap(mapName);
    }


    public void writeToMap(int testNumber) {
        map.put("Test-" + memberId, "" + testNumber);
    }

    /**
     * stop atomix, remove all event listeners and close open resources
     */
    public void stop() {
        log("Stopping " + memberId, 4);
        map.removeListener(this::event);
        map.close();
        election.removeListener(this::leadershipChanged);
        atomix.stop().join();
    }

    /**
     * Removes from leader election
     */
    public void removeFromLeaderElection() {
        log("Withdrawing " + memberId + " from leader election", 4);
        if (memberId.equals(currentLeader)) {
            election.withdraw(memberId);
            //var newMembership = election.run(memberId);
        }
//        var leaderShip = election.run(memberId);
//        if (leaderShip.leader().id() == memberId) // I'm the leader

    }

    /**
     * promotes current member to leader
     */
    public void promoteToLeader() {
        log("Promoting " + memberId + " to leader", 4);
        election.anoint(memberId);
    }

    private void log(String message, int severity) {
        logger.Log(message, severity);
    }

    private void leadershipChanged(LeadershipEvent<String> event) {
        var newLeader = event.newLeadership().leader().id();
        currentLeader = newLeader;
        var oldLeaderShip = event.oldLeadership();
        var oldLeader = event.oldLeadership().leader().id();
        logger.Log(memberId + ": Leadership has changed - new leader: " + newLeader + " old leader " + oldLeader, 4);
    }

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
