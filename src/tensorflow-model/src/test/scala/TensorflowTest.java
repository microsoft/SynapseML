// Copyright (C) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License. See LICENSE in project root for information.

import org.tensorflow.Graph;
import org.tensorflow.Session;
import org.tensorflow.Tensor;
import org.tensorflow.TensorFlow;


public class TensorflowTest {
    public static void main(String[] args) throws Exception {
          String modelPath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/inception5h";
          String applePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/RedApple.jpg";
          String pineapplePath = "/home/houssam/externship/mmlspark/src/tensorflow-model/src/test/LabelImage_data/olpineapple.jpeg";

          LabelImage.main(new String[] {modelPath,applePath});
          LabelImage.main(new String[] {modelPath,pineapplePath});

//        try (Graph g = new Graph()) {
//            final String value = "Hello from " + TensorFlow.version();
//
//            // Construct the computation graph with a single operation, a constant
//            // named "MyConst" with a value "value".
//            try (Tensor t = Tensor.create(value.getBytes("UTF-8"))) {
//                // The Java API doesn't yet include convenience functions for adding operations.
//                g.opBuilder("Const", "MyConst").setAttr("dtype", t.dataType()).setAttr("value", t).build();
//            }
//
//            // Execute the "MyConst" operation in a Session.
//            try (Session s = new Session(g);
//                 Tensor output = s.runner().fetch("MyConst").run().get(0)) {
//                System.out.println(new String(output.bytesValue(), "UTF-8"));
//            }
//        }
    }

}

