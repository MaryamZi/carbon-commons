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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.Member;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.ntask.common.TaskException;
import org.wso2.carbon.ntask.core.TaskManager;
import org.wso2.carbon.ntask.core.impl.clustered.rpc.TaskCall;
import org.wso2.carbon.ntask.core.internal.TasksDSComponent;
import org.wso2.carbon.ntask.core.service.TaskService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The cluster group communicator interface used for cluster communication.
 */
public abstract class ClusterGroupCommunicator {
    private static Log log = LogFactory.getLog(ClusterGroupCommunicator.class);

    public static final String NTASK_P2P_COMM_EXECUTOR = "__NTASK_P2P_COMM_EXECUTOR__";

    protected static final String TASK_SERVER_STARTUP_COUNTER = "__TASK_SERVER_STARTUP_COUNTER__";

    protected static final int MISSING_TASKS_ON_ERROR_RETRY_COUNT = 3;

    protected static final String CARBON_TASKS_MEMBER_ID_MAP = "__CARBON_TASKS_MEMBER_ID_MAP__";

    public static final String TASK_SERVER_COUNT_SYS_PROP = "task.server.count";

    protected TaskService taskService;

    protected HazelcastInstance hazelcastInstance; //TODO: move to HazelcastClusterGroupCommunicator once Hz is completely removed

    public static final int MAX_NODE_ID_READ_ATTEMPTS = 4;

    protected static Map<String, ClusterGroupCommunicator> communicatorMap = new HashMap<>();

    protected String taskType;

    public static ClusterGroupCommunicator getInstance(String taskType) throws TaskException {
        throw new TaskException("getInstance method has not been defined in subclass ", null);
    }

    public abstract void addMyselfToGroup();

    public HazelcastInstance getHazelcast() {
        return hazelcastInstance;
    }

    public String getStartupCounterName() {
        return TASK_SERVER_STARTUP_COUNTER + getTaskType();
    }

    public String getTaskType() {
        return taskType;
    }

    protected abstract Member getMemberFromId(String id) throws TaskException;

    public void checkServers() throws TaskException {
        int serverCount = this.getTaskService().getServerConfiguration().getTaskServerCount();
        if (serverCount != -1) {
            log.info("Waiting for " + serverCount + " [" + this.getTaskType() + "] task executor nodes...");
            try {
                /* with this approach, lets say the server count is 3, and after all 3 server comes up,
                 * and tasks scheduled, if two nodes go away, and one comes up, it will be allowed to start,
                 * even though there aren't 3 live nodes, which would be the correct approach, if the whole
                 * cluster goes down, then, you need again for all 3 of them to come up */
                while (this.getHazelcast().getAtomicLong(this.getStartupCounterName()).get() < serverCount) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                throw new TaskException("Error in waiting for task [" + this.getTaskType() + "] executor nodes: " +
                        e.getMessage(), TaskException.Code.UNKNOWN, e);
            }
            log.info("All task servers activated for [" + this.getTaskType() + "].");
        }
    }

    public TaskService getTaskService() {
        return taskService;
    }

    public abstract List<String> getMemberIds() throws TaskException;

    public String getMemberId() {
        return this.getIdFromMember(this.getHazelcast().getCluster().getLocalMember());
    }

    protected String getIdFromMember(Member member) {
        return member.getUuid();
    }

    public abstract boolean isLeader();

    public <V> V sendReceive(String memberId, TaskCall<V> taskCall) throws TaskException {
        IExecutorService es = this.getHazelcast().getExecutorService(NTASK_P2P_COMM_EXECUTOR);
        Future<V> taskExec = es.submitToMember(taskCall, this.getMemberFromId(memberId));
        try {
            return taskExec.get();
        } catch (Exception e) {
            throw new TaskException("Error in cluster message send-receive: " + e.getMessage(),
                    TaskException.Code.UNKNOWN, e);
        }
    }

    protected void scheduleAllMissingTasks() throws TaskException {
        for (TaskManager tm : getTaskService().getAllTenantTaskManagersForType(this.getTaskType())) {
            if (tm instanceof ClusteredTaskManager) {
                this.scheduleMissingTasksWithRetryOnError((ClusteredTaskManager) tm);
            }
        }
    }

    protected abstract void scheduleMissingTasksWithRetryOnError(ClusteredTaskManager tm);

}
