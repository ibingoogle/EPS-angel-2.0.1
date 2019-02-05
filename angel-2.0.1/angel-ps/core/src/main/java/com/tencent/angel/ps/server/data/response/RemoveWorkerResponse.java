/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ps.server.data.response;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The result of the remove worker rpc request.
 */
public class RemoveWorkerResponse extends Response {

    private static final Log LOG = LogFactory.getLog(RemoveWorkerResponse.class);
    /**
     * number of rest workers
     */
    public int numRestWorkers;

    /**
     * Create a new RemoveWorkerResponse.
     *
     * @param responseType response type
     * @param detail       detail response information
     * @param numRestWorkers       information about the number of rest workers
     */
    public RemoveWorkerResponse(ResponseType responseType, String detail,
                                int numRestWorkers) {
        super(responseType, detail);
        this.numRestWorkers = numRestWorkers;
        LOG.info("number of rest workers = " + this.numRestWorkers);
    }

    /**
     * Create a new GetClocksResponse.
     */
    public RemoveWorkerResponse() {
        super();
        numRestWorkers = -1;
    }

    @Override public void serialize(ByteBuf buf) {
        super.serialize(buf);
        buf.writeInt(numRestWorkers);
    }

    @Override public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        this.numRestWorkers = buf.readInt();
    }

    @Override public int bufferLen() {
        int len = super.bufferLen();
        len += 4;
        return len;
    }

    @Override public void clear() {
        numRestWorkers = -1;
    }
}
