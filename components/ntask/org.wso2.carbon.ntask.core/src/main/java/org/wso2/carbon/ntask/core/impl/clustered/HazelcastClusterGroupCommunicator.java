/**
 *  Copyright (c) 2012, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.wso2.carbon.ntask.core.impl.clustered;

import com.hazelcast.core.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.ntask.common.TaskException;
import org.wso2.carbon.ntask.common.TaskException.Code;
import org.wso2.carbon.ntask.core.TaskManager;
import org.wso2.carbon.ntask.core.impl.clustered.rpc.TaskCall;
import org.wso2.carbon.ntask.core.internal.TasksDSComponent;
import org.wso2.carbon.ntask.core.service.TaskService;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * This class represents the Hazelcast based cluster group communicator used by clustered task
 * managers.
 */
public class HazelcastClusterGroupCommunicator extends ClusterGroupCommunicator implements MembershipListener {

    private static final Log log = LogFactory.getLog(HazelcastClusterGroupCommunicator.class);

    private Map<String, Member> membersMap;

    public static HazelcastClusterGroupCommunicator getInstance(String taskType) throws TaskException {
        if (communicatorMap.containsKey(taskType)) {
            return (HazelcastClusterGroupCommunicator) communicatorMap.get(taskType);
        } else {
            synchronized (communicatorMap) {
                if (!communicatorMap.containsKey(taskType)) {
                    communicatorMap.put(taskType, new HazelcastClusterGroupCommunicator(taskType));
                }
                return (HazelcastClusterGroupCommunicator) communicatorMap.get(taskType);
            }
        }
    }

    private HazelcastClusterGroupCommunicator(String taskType) throws TaskException {
        this.taskType = taskType;
        this.taskService = TasksDSComponent.getTaskService();
        this.hazelcastInstance = TasksDSComponent.getHazelcastInstance();
        if (this.getHazelcast() == null) {
            throw new TaskException("HazelcastClusterGroupCommunicator cannot initialize, " +
            		"Hazelcast is not initialized", Code.CONFIG_ERROR);
        }
        this.getHazelcast().getCluster().addMembershipListener(this);
        this.refreshMembers();
    }

    private void refreshMembers() {
    	/* create a distributed map to track the members in the current group */
        this.membersMap = this.getHazelcast().getMap(CARBON_TASKS_MEMBER_ID_MAP + "#" + taskType);
        /* check and remove expired members */
        this.checkAndRemoveExpiredMembers();
    }

    @Override
    public void addMyselfToGroup() {
    	Member member = this.getHazelcast().getCluster().getLocalMember();
        /* add myself to the queue */
        this.membersMap.put(this.getIdFromMember(member), member);
        /* increment the task server count */
        this.getHazelcast().getAtomicLong(this.getStartupCounterName()).incrementAndGet();
    }

    /**
     * This method checks and removes older non-existing nodes from the map. This can happen
     * when there are multiple servers, for example a server which has ntask component and another server
     * which just has hazelcast. When the server with ntask shutsdown, the other server still contain the
     * queue, and that queue contain the member id of the earlier server which had ntask. So at startup,
     * we have to check the queue and see if there are non-existing members by comparing it to the current
     * list of active members.
     */
    private void checkAndRemoveExpiredMembers() {
        Set<Member> existingMembers = this.getHazelcast().getCluster().getMembers();
        Iterator<Map.Entry<String, Member>> itr = this.membersMap.entrySet().iterator();
        List<String> removeList = new ArrayList<>();
        Map.Entry<String, Member> currentEntry;
        while (itr.hasNext()) {
            currentEntry = itr.next();
            if (!existingMembers.contains(currentEntry.getValue())) {
                removeList.add(currentEntry.getKey());
            }
        }
        for (String key : removeList) {
            this.membersMap.remove(key);
        }
    }

    @Override
    protected Member getMemberFromId(String id) throws TaskException {
        Member member = this.membersMap.get(id);
        if (member == null) {
            /* this is probably because of an edge case, where a member has just gone away when we
             * trying to access this member, this must be handled in the upper layers by retrying
             * the root operation */
            throw new TaskException("The member with id: " + id + " does not exist", Code.UNKNOWN);
        }
        return member;
    }

    @Override
    public synchronized List<String> getMemberIds() throws TaskException {
        return new ArrayList<String>(this.membersMap.keySet());
    }

    @Override
    public boolean isLeader() {
        if (this.getHazelcast().getLifecycleService().isRunning()) {
        	String id;
        	/* as per documentation, getMembers return the oldest member first */
            for (Member member : this.getHazelcast().getCluster().getMembers()) {
            	id = this.getIdFromMember(member);
            	if (this.membersMap.containsKey(id)) {
            		return this.getMemberId().equals(id);
            	}
            }
        }
        return false;
    }

    @Override
    public void memberAdded(MembershipEvent event) {
        /* ignored; the member addition for this group is handled in
         * the initialization of the cluster group communicator */
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
                log.error("Encountered error(s) in scheduling missing tasks ["
                        + tm.getTaskType() + "][" + tm.getTenantId() + "]:- \n" +
                        e.getMessage() + "\n" + (retry ? "Retrying [" +
                        ((MISSING_TASKS_ON_ERROR_RETRY_COUNT - count) + 1) + "]..." : "Giving up."));
                if (retry) {
                   /* coming up is a retry operation, lets do some cleanup */
        		   this.cleanupTaskCluster();
                }
            }
            count--;
        }
    }

    /**
     * Cleanup up possible inconsistencies that can happen because of cluster instability,
     * e.g. cluster messages being lost etc..
     */
    private void cleanupTaskCluster() {
    	this.refreshMembers();
    }

    @Override
    public void memberRemoved(MembershipEvent event) {
        if (this.getHazelcast().getLifecycleService().isRunning()) {
            String id = this.getIdFromMember(event.getMember());
            this.membersMap.remove(id);
            try {
                if (this.isLeader()) {
                    log.info("Task [" + this.getTaskType() + "] member departed [" + event.getMember().toString()
                            + "], rescheduling missing tasks...");
                    this.scheduleAllMissingTasks();
                }
            } catch (TaskException e) {
                log.error("Error in scheduling missing tasks [" + this.getTaskType() + "]: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
    	/* ignored */
    }

    public Map<String, Member> getMemberMap() {
        return membersMap;
    }

}
