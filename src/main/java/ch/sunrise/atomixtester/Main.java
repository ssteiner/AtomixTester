package ch.sunrise.atomixtester;

import java.util.ArrayList;

public class Main {

    public static void main(String[] args) {

        var logger = new DummyLogger();

        var config = new ClusterConfiguration();

        var instance1 = new ClusterAddress();
        instance1.setIpOrHost("localhost");
        instance1.setPort(2050);
        instance1.setMemberId("member1");

        var instance2 = new ClusterAddress();
        instance2.setIpOrHost("localhost");
        instance2.setPort(2051);
        instance2.setMemberId("member2");

        var addresses = new ArrayList<ClusterAddress>();
        addresses.add(instance1);
        addresses.add(instance2);

        config.setMembers(addresses);

        var main = new ClusterServer(config, instance1.getMemberId(), logger);
        main.initialize();
        main.start();

        var backup = new ClusterServer(config, instance2.getMemberId(), logger);
        backup.initialize();
        backup.start();

        main.runTest();
        backup.runTest();

//        main.stop();
//        backup.runTest(1); //=> throws a PrimitiveExceptionTimeout
//        main.initialize();
//        main.start();

        backup.stop();
        main.runTest(1);
        backup.initialize();
        backup.start();
        main.runTest(2);
        backup.runTest(2);

        main.stop();
        backup.stop();

    }
}
