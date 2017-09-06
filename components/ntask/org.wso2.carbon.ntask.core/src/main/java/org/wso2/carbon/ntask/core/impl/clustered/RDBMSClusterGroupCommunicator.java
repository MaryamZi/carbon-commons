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

package org.wso2.carbon.ntask.core.impl.clustered;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.cluster.coordinator.commons.ClusterCoordinator;
import org.wso2.carbon.cluster.coordinator.commons.node.NodeDetail;
import org.wso2.carbon.ntask.common.TaskException;
import org.wso2.carbon.ntask.core.impl.clustered.rdbms.coordination.RDBMSCoordinationMemberEventListener;
import org.wso2.carbon.ntask.core.internal.TasksDSComponent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * This class represents the RDBMS based cluster group communicator used by clustered task
 * managers.
 * TODO: remove implementing MemershipListener when Hz is completely removed.
 */
public class RDBMSClusterGroupCommunicator extends ClusterGroupCommunicator implements MembershipListener {

    private static final Log logger = LogFactory.getLog(RDBMSClusterGroupCommunicator.class);

    private static ClusterCoordinator clusterCoordinator;

    private RDBMSCoordinationMemberEventListener rdbmsCoordinationMemberEventListener;

    //TODO:checkkkkk
    public static RDBMSClusterGroupCommunicator getInstance(String taskType) throws TaskException {
        if (communicatorMap.containsKey(taskType)) {
            return (RDBMSClusterGroupCommunicator) communicatorMap.get(taskType);
        } else {
            synchronized (communicatorMap) {
                if (!communicatorMap.containsKey(taskType)) {
                    communicatorMap.put(taskType, new RDBMSClusterGroupCommunicator(taskType));
                }
                return (RDBMSClusterGroupCommunicator) communicatorMap.get(taskType);
            }
        }
    }

    private RDBMSClusterGroupCommunicator(String taskType) throws TaskException {
        this.taskType = taskType;
        hazelcastInstance = TasksDSComponent.getHazelcastInstance();
        taskService = TasksDSComponent.getTaskService();
        clusterCoordinator = TasksDSComponent.getClusterCoordinator();
        if (this.getHazelcast() == null) {
            throw new TaskException("RDBMSClusterGroupCommunicator cannot initialize, " +
                    "Hazelcast is not initialized", TaskException.Code.CONFIG_ERROR);
        }
        rdbmsCoordinationMemberEventListener = new RDBMSCoordinationMemberEventListener(this);
        rdbmsCoordinationMemberEventListener.setGroupId(getTaskType());
        clusterCoordinator.registerEventListener(taskType, rdbmsCoordinationMemberEventListener);
        getHazelcast().getCluster().addMembershipListener(this);
    }

    @Override
    public void addMyselfToGroup() {
        /* increment the task server count */
        this.getHazelcast().getAtomicLong(this.getStartupCounterName()).incrementAndGet();
        HashMap<String, Object> propertiesMap = new HashMap<>();
        String hazelcastNodeID = this.getMemberId();
        propertiesMap.put("HazelcastID", hazelcastNodeID);
        clusterCoordinator.joinGroup(getTaskType(), propertiesMap);
    }

    @Override
    protected Member getMemberFromId(String id) throws TaskException {
        Set<Member> members = this.getHazelcast().getCluster().getMembers();
        for (Member member:members) {
            if (member.getUuid().equals(id)) {
                return member;
            }
        }
        throw new TaskException("The member with id: " + id + " does not exist", TaskException.Code.UNKNOWN);
    }

    @Override
    public synchronized List<String> getMemberIds() throws TaskException {
        List<String> memberIds = new ArrayList<>();
        Set<Member> members = this.getHazelcast().getCluster().getMembers();
        for (Member member:members) {
            memberIds.add(member.getUuid());
        }
        return memberIds;
    }

    @Override
    public boolean isLeader() {
        NodeDetail leaderNodeDetail = clusterCoordinator.getLeaderNode(getTaskType());
        String leaderNodeHazelcastID = leaderNodeDetail.getpropertiesMap().get("HazelcastID").toString();
        return leaderNodeHazelcastID.equals(getMemberId());
    }

    @Override
    public void memberAdded(MembershipEvent event) {
        //RDBMS coordination will handle the member added event.
    }

    public void memberAdded(NodeDetail nodeDetail) {
        logger.info("Member Joined: " + nodeDetail.getGroupId() + "." + nodeDetail.getNodeId());
    }

    @Override
    protected void scheduleMissingTasksWithRetryOnError(ClusteredTaskManager tm) {
        int count = MISSING_TASKS_ON_ERROR_RETRY_COUNT;
        while (count > 0) {
            try {
                tm.scheduleMissingTasks();
                break;
            } catch (TaskException e) {
                boolean retry = (count > 1);
                logger.error("RDBMS Encountered error(s) in scheduling missing tasks ["
                        + tm.getTaskType() + "][" + tm.getTenantId() + "]:- \n" +
                        e.getMessage() + "\n" + (retry ? "Retrying [" +
                        ((MISSING_TASKS_ON_ERROR_RETRY_COUNT - count) + 1) + "]..." : "Giving up."));
            }
            count--;
        }
    }

    @Override
    public void memberRemoved(MembershipEvent event) {
        //RDBMS coordination will handle the member removed event.
    }

    public void memberRemoved(NodeDetail nodeDetail) {
        try {
            if (this.isLeader()) {
                logger.info("RDBMS Task [" + this.getTaskType() + "] member departed [" + nodeDetail.getNodeId()
                        + "], rescheduling missing tasks...");
                this.scheduleAllMissingTasks();
            }
        } catch (TaskException e) {
            logger.error("RDBMS Error in scheduling missing tasks [" + this.getTaskType() + "]: " + e.getMessage(), e);
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
        /* ignored */
    }
}
