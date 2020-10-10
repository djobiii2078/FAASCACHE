/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package runtime.actionContainers

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import common.WskActorSystem
import spray.json._
import DefaultJsonProtocol._

@RunWith(classOf[JUnitRunner])
class Python3AiActionContainerTests extends PythonActionContainerTests with WskActorSystem {

  override lazy val imageName = "python3aiaction"

  it should "run tensorflow" in {
    val (out, err) = withActionContainer() { c =>
      val code =
        """
          |import tensorflow as tf
          |def main(args):
          |   # Initialize two constants
          |   x1 = tf.constant([1,2,3,4])
          |   x2 = tf.constant([5,6,7,8])
          |
          |   # Multiply
          |   result = tf.multiply(x1, x2)
          |
          |   # Initialize Session and run `result`
          |   with tf.Session() as sess:
          |       output = sess.run(result)
          |       print(output)
          |       return { "response": output.tolist() }
        """.stripMargin

      val (initCode, res) = c.init(initPayload(code))
      initCode should be(200)

      val (runCode, runRes) = c.run(runPayload(JsObject()))
      runCode should be(200)

      runRes shouldBe defined
      runRes should be(Some(JsObject("response" -> List(5, 12, 21, 32).toJson)))
    }
  }

  it should "run pytorch" in {
    val (out, err) = withActionContainer() { c =>
      val code =
        """
          |import torch
          |import torchvision
          |import torch.nn as nn
          |import numpy as np
          |import torchvision.transforms as transforms
          |def main(args):
          |   # Create a numpy array.
          |   x = np.array([1,2,3,4])
          |
          |   # Convert the numpy array to a torch tensor.
          |   y = torch.from_numpy(x)
          |
          |   # Convert the torch tensor to a numpy array.
          |   z = y.numpy()
          |   return { "response": z.tolist()}
        """.stripMargin

      val (initCode, res) = c.init(initPayload(code))
      initCode should be(200)

      val (runCode, runRes) = c.run(runPayload(JsObject()))
      runCode should be(200)

      runRes shouldBe defined
      runRes should be(Some(JsObject("response" -> List(1, 2, 3, 4).toJson)))
    }
  }

}
