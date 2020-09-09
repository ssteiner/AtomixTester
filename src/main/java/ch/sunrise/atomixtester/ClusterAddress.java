package ch.sunrise.atomixtester;

public class ClusterAddress {
    private String IpOrHost;

    private int port;

    public String getIpOrHost() {
        return IpOrHost;
    }

    public void setIpOrHost(String ipOrHost) {
        IpOrHost = ipOrHost;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    private String MemberId;

    public String getMemberId() {
        return MemberId;
    }

    public void setMemberId(String memberId) {
        MemberId = memberId;
    }
}
