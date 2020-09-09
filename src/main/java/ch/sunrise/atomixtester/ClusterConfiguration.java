package ch.sunrise.atomixtester;

import java.util.List;

public class ClusterConfiguration {
    private List<ClusterAddress> Members;

    public List<ClusterAddress> getMembers() {
        return Members;
    }

    public void setMembers(List<ClusterAddress> members) {
        Members = members;
    }
}
