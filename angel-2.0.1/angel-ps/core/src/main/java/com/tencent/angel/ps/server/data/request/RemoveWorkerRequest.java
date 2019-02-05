package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;


/**
 * Remove worker request.
 */
public class RemoveWorkerRequest extends Request {
   private ParameterServerId serverId;
   public int serverIndex;
   public int workerIndex;

    /**
     * Create a new RemoveWorkerRequest.
     *
     * @param serverId parameter server id
     */
    public RemoveWorkerRequest (ParameterServerId serverId, int workerIndex) {
        super(new RequestContext());
        this.serverId = serverId;
        this.serverIndex = this.serverId.getIndex();
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

    @Override public void serialize(ByteBuf buf) {
        super.serialize(buf);
        buf.writeInt(serverIndex);
        buf.writeInt(workerIndex);
    }

    @Override public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        this.serverIndex = buf.readInt();
        this.workerIndex = buf.readInt();
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
