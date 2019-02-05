package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.TransportMethod;


/**
 * Remove worker request.
 */
public class RemoveWorkerRequest extends Request {
   private ParameterServerId serverId;
   public int workerIndex;

    /**
     * Create a new RemoveWorkerRequest.
     *
     * @param serverId parameter server id
     */
    public RemoveWorkerRequest (ParameterServerId serverId, int workerIndex) {
        super(new RequestContext());
        this.serverId = serverId;
        this.workerIndex = workerIndex;
    }

    /**
     * Create a new RemoveWorkerRequest.
     */
    public RemoveWorkerRequest() {
        super();
    }

    @Override public int getEstimizeDataSize() {
        return 0;
    }

    @Override public TransportMethod getType() {
        return TransportMethod.REMOVE_WORKER;
    }

    /**
     * Get the rpc dest ps
     *
     * @return the rpc dest ps
     */
    public ParameterServerId getServerId() {
        return serverId;
    }
}
