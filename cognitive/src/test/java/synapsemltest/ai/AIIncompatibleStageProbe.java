// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

package synapsemltest.ai;

public final class AIIncompatibleStageProbe {
    static {
        System.setProperty("synapseml.aiprompt.test.incompatible.initialized", "true");
    }

    public AIIncompatibleStageProbe(String uid) {
        System.setProperty("synapseml.aiprompt.test.incompatible.constructed", uid);
    }
}
