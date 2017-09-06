/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.ntask.core.impl.clustered.rdbms.coordination;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import org.wso2.carbon.cluster.coordinator.commons.MemberEventListener;
import org.wso2.carbon.cluster.coordinator.commons.node.NodeDetail;
import org.wso2.carbon.ntask.core.impl.clustered.RDBMSClusterGroupCommunicator;

import java.util.Set;

/**
 * Default implementation of MemberEventListener for RDBMS based Coordination.
 */
public class RDBMSCoordinationMemberEventListener extends MemberEventListener {

    private RDBMSClusterGroupCommunicator rdbmsClusterGroupCommunicator;
    private HazelcastInstance hazelcastInstance;
    private Cluster cluster;
    private Set<Member> members;

    public RDBMSCoordinationMemberEventListener(RDBMSClusterGroupCommunicator rdbmsClusterGroupCommunicator) {
        this.rdbmsClusterGroupCommunicator = rdbmsClusterGroupCommunicator;
    }

    @Override
    public void memberAdded(NodeDetail nodeDetail) {
        rdbmsClusterGroupCommunicator.memberAdded(nodeDetail);
    }

    @Override
    public void memberRemoved(NodeDetail nodeDetail) {
        rdbmsClusterGroupCommunicator.memberRemoved(nodeDetail);
    }

    @Override
    public void coordinatorChanged(NodeDetail nodeDetail) {
        //Nothing to be done here.
    }
}
