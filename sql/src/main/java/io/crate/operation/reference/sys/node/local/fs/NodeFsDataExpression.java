/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.reference.sys.node.local.fs;


import io.crate.monitor.ExtendedFsStats;
import io.crate.operation.reference.NestedObjectExpression;
import io.crate.operation.reference.sys.SysStaticObjectArrayReference;
import io.crate.operation.reference.sys.node.local.SysNodeObjectReference;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

class NodeFsDataExpression extends SysStaticObjectArrayReference {

    private final ExtendedFsStats extendedFsStats;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    NodeFsDataExpression(ExtendedFsStats extendedFsStats) {
        this.extendedFsStats = extendedFsStats;
    }

    @Override
    protected List<NestedObjectExpression> getChildImplementations() {
        if (!initialized.getAndSet(true)) {
            addChildImplementations();
        }
        return childImplementations;
    }

    private void addChildImplementations() {
        for (ExtendedFsStats.Info info : extendedFsStats) {
            childImplementations.add(new NodeFsDataChildExpression(info));
        }
    }


    private class NodeFsDataChildExpression extends SysNodeObjectReference {

        private static final String DEV = "dev";
        private static final String PATH = "path";

        NodeFsDataChildExpression(final ExtendedFsStats.Info fsInfo) {
            childImplementations.put(DEV, new ChildExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    return fsInfo.dev();
                }
            });
            childImplementations.put(PATH, new ChildExpression<BytesRef>() {
                @Override
                public BytesRef value() {
                    return fsInfo.path();
                }
            });
        }
    }
}
