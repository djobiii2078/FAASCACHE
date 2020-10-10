package org.apache.openwhisk.core.controller.ml

import java.io.{File, FileInputStream, ObjectInputStream}
import java.nio.file.{Paths, Files}

// this is imported from the JAR in the same directory (which is set as a dependency for the module openwhisk.common)
import fr.irit.sepia.slimfaas.mlstudy.argumentprocessors.ArgumentProcessorFactory
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.MemoryLimit


import org.apache.openwhisk.core.entity.size.SizeLong
import spray.json.{JsBoolean, JsNull, JsNumber, JsObject, JsString}
import weka.classifiers.Classifier
import weka.core.converters.{ArffLoader, ArffSaver}
import weka.core.{Attribute, DenseInstance, Instance, Instances, SerializationHelper}

import weka.classifiers.trees.J48

import scala.collection.JavaConverters._
import java.util.{ArrayList}
// import _root_.java.nio.file.Path

/** Trait for all objects that can compute a new memory limit. */
trait MemorySlimmer {
  /** Compute a new, slimmed down memory limit. */
  def slim(action: WhiskActionMetaData, payload: Option[JsObject],memory: Option[String])(logging: Logging): Int
}

/** Dummy memory slimmer that does not change the memory limit. */
object NoChangeMemorySlimmer extends MemorySlimmer {
  override def slim(action: WhiskActionMetaData, payload: Option[JsObject], memory: Option[String])(logging: Logging): Int = {
    logging.info(this, s"Memory slimmed down for action $action: no change (${action.limits.memory}MiB)")
    action.limits.memory.megabytes
  }
}



/** Object managing instances for every created function **/ 
case class FinalMemSlimmer() extends MemorySlimmer {

  type ArgumentsProcessorFactories = Map[Integer, Map[String, ArgumentProcessorFactory]]

  

  final val DATASET_SIZE_TO_TRAIN = 1000;// THe size of the dataset that triggers the training for a function's model
  final val NB_ATTRIBUTES = 62
  // sharp_blur11 and wand_edge12 are the duplicates
  final val FUNCTION_NAMES_TO_IDS = List(
    "wand_denoise",
    "sharp_blur",
    "sharp_resize",
    "wand_sepia",
    "sharp_sepia",
    "wand_resize",
    "wand_edge",
    "sharp_convert",
    "wand_rotate",
    "wand_blur",
    "sharp_blur11",
    "wand_edge12").zipWithIndex.map { case (name, index) => (name, index + 1) }.toMap
  final val FUNCTION_ARGUMENTS = Map(
    1 -> List("threshold", "softness"),
    2 -> List("sigma"),
    3 -> List("width"),
    4 -> List(),
    5 -> List(),
    6 -> List("width"),
    7 -> List(),
    8 -> List("format"),
    9 -> List("angle"),
    10 -> List("sigma"),
    11 -> List("sigma"),
    12 -> List())

    final val functionArgumentsProcessors = loadFunctionArguments("/argprocs")


   def loadFunctionArguments(inpath: String): ArgumentsProcessorFactories = {
    /* Here we load the serialized ArgumentsProcessorFactories from file, converting them to proper Scala objects, and also
     * mapping each factory to the name of its argument using FUNCTION_ARGUMENTS. This is specific to using the offline model.
     */
    val ois = new ObjectInputStream(new FileInputStream(inpath))
    val ret = ois.readObject
      .asInstanceOf[java.util.Map[Integer, java.util.List[ArgumentProcessorFactory]]]
      .asScala
      .map { case (index, value) => index -> FUNCTION_ARGUMENTS(index).zip(value.asScala).toMap }
      .toMap
    ois.close()

    ret
  }

  def canPredict(action: WhiskActionMetaData): Boolean = {
    
    def getFunctionId = {
      action.annotations.get("function") match {
        case Some(jsValue) =>
          jsValue match {
            case JsNumber(id) => id.doubleValue()
            case value =>
              throw new Exception(s"""Getting function ID from annotations failed: got JsValue "$value"""")
          }
        case None =>
          throw new Exception("""Getting function ID from annotations failed: no annotation "function" found""")
      }
    }

    val modelExists = Files.exists(Paths.get(s"$getFunctionId.model"))
    modelExists

  }

  override def slim(action: WhiskActionMetaData, payload: Option[JsObject], memory: Option[String])(logging: Logging): Int = {
    //For the moment, we correctly manage python2.7 actions for the integration with RAMCloud
    //Meanwhile, our predictor works fine for js actions, the main logic behind this function 
    //is only active for python2.7 actions

    def getLanguage = {
      action.exec.kind.split(':').head match {
        case "python" => "python"
        case "nodejs" => "javascript"
        case "blackbox" =>
          List("python", "nodejs").find(action.exec.asInstanceOf[BlackBoxExecMetaData].image.name.contains(_)) match {
            case Some(language) =>
              language match {
                case "python" => "python"
                case "nodejs" => "javascript"
              }
            case None =>
              throw new Exception(s"""Getting the language from the blackbox image "${action.exec
                .asInstanceOf[BlackBoxExecMetaData]
                .image
                .name}" failed""")
          }
        case kind => throw new Exception(s"""Getting the language from the kind failed: unknown kind "$kind"""")
      }
    }

    def getFunctionId = {
      action.annotations.get("function") match {
        case Some(jsValue) =>
          jsValue match {
            case JsNumber(id) => id.doubleValue()
            case value =>
              throw new Exception(s"""Getting function ID from annotations failed: got JsValue "$value"""")
          }
        case None =>
          throw new Exception("""Getting function ID from annotations failed: no annotation "function" found""")
      }
    }

    //Check the language used by the action used and silently drop 
    //if it is not handled yet  
    getLanguage match {
        case "nodejs" | "javascript" =>
        {
                logging.info(this,s"Silently dropping this for not handled function. Try with a python:2.7 action")
                None 
        }

    }

    //Construct the instance for the new function 
    val inst = new DenseInstance(NB_ATTRIBUTES)
    val arguments = payload.getOrElse(JsObject.empty).fields.mapValues {
        case JsString(arg)  => arg
        case JsNumber(arg)  => arg.toString
        case JsBoolean(arg) => arg.toString
        case JsNull         => ""
        case value          => throw new Exception(s"""Getting list of arguments from payload failed on JsValue "$value"""")
      }
    
    inst.setValue(0, arguments("input_size"))
    var index = 0
    functionArgumentsProcessors(FUNCTION_NAMES_TO_IDS(action.name.asString)).foreach {
        case (_, null) => // ignored argument
        case (argname, argprocfact) =>
          val argproc = argprocfact.build(arguments(argname.toString))
          inst.setValue(argproc.getProcessedAttributeId, argproc.getProcessedValue)
          index = argproc.getProcessedAttributeId+1
    }

    //Check if for the function id, a model has already been done 
    //Later on, this code will be updated to use the datastore CouchDB for checking 

    Files.exists(Paths.get(s"$getFunctionId.model")) match {
      case false => {
          //In this case, we continue to dope our dataset with new entries 
          //And once we attain DATASET_SIZE_TO_TRAIN we train and serialize the classifier 
          def loadDataStructure(inpath: String): Instances = {
                val fin = new FileInputStream(inpath)
                val arffLoader = new ArffLoader
                arffLoader.setSource(fin)
                val ret = arffLoader.getDataSet
                fin.close()

                ret
           }

           Files.exists(Paths.get(s"$getFunctionId.arff")) match {
            case false => {
               val arffSaver = new ArffSaver
               val atts = new ArrayList[Attribute] 
               
               //Get the arguments passed to the function 
               //arguments.keySet.foreach((item: String)=> atts.addElement(new Attribute(item.toString)))
               arguments.keySet.foreach((item: String)=> atts.add(new Attribute(s"$item")))
//             atts.addElement(new Attribute("input_size"))

               
               arffSaver.setInstances(new Instances(s"Dataset.$getFunctionId", atts, 0))
               arffSaver.setFile(new File(s"$getFunctionId.arff"))
               arffSaver.writeBatch()
               0 //Dummy return to comply with void Unit 

            }

            case true => {
              var funcDataset = loadDataStructure(s"$getFunctionId.arff")
              val arffSaver = new ArffSaver

              inst.setValue(index, memory.getOrElse(""))
              funcDataset.add(inst)
              arffSaver.setInstances(funcDataset)
              //arffSaver.setInstances(inst)
              arffSaver.setFile(new File(s"$getFunctionId.arff"))
              arffSaver.writeBatch()

              //Check if we can build a classifier to use for future predictions 
              // Update classifier mechanism not working yet 
              // Need to ingest real prediction -- @TODO

              funcDataset = loadDataStructure(s"$getFunctionId.arff")
              if(funcDataset.size() > DATASET_SIZE_TO_TRAIN)
              {
                funcDataset.setClassIndex(funcDataset.numAttributes()-1)
                val cls = new J48
                cls.buildClassifier(funcDataset)
                SerializationHelper.write(s"$getFunctionId.model",cls)
              }

              0

            }
          }
      }
      case true => {
        //Model already exists, just need to predict the memory and bim 
        //Let's go over with it  
        val modelFileName = s"$getFunctionId.model"
        logging.info(this,s"Model for function $modelFileName exists. Just predict the memory")
        val loadedModel = SerializationHelper.read(modelFileName).asInstanceOf[Classifier]
        
        //CLassify the object 
        //Predict the memory to be used 

      try {
         val memPred = loadedModel.classifyInstance(inst).ceil.longValue().B

         logging.info(this, s"Predicted memory: ${memPred.toMB} MiB")

        // Here we check that the regressed value is legal. The model predicts values between 128MiB and 3GiB which is a much
        // wider range than the default configuration of OW, so change that when deploying OW.
        MemoryLimit(
          if (memPred < MemoryLimit.MIN_MEMORY) MemoryLimit.MIN_MEMORY
          else if (memPred > MemoryLimit.MAX_MEMORY) MemoryLimit.MAX_MEMORY
          else memPred).megabytes
      } catch {
        case err: Exception =>
          logging.error(this, s"Predicting the memory limit failed\n${err.printStackTrace()}")
          action.limits.memory.megabytes
      }

      }
    }
    

  }
}

/** Memory slimmer that uses a pre-learned linear SVM model. */
object LinearSVMMemorySlimmer extends MemorySlimmer {
  type ArgumentsProcessorFactories = Map[Integer, Map[String, ArgumentProcessorFactory]]

  final val NB_ATTRIBUTES = 62
  // sharp_blur11 and wand_edge12 are the duplicates
  final val FUNCTION_NAMES_TO_IDS = List(
    "wand_denoise",
    "sharp_blur",
    "sharp_resize",
    "wand_sepia",
    "sharp_sepia",
    "wand_resize",
    "wand_edge",
    "sharp_convert",
    "wand_rotate",
    "wand_blur",
    "sharp_blur11",
    "wand_edge12").zipWithIndex.map { case (name, index) => (name, index + 1) }.toMap
  final val FUNCTION_ARGUMENTS = Map(
    1 -> List("threshold", "softness"),
    2 -> List("sigma"),
    3 -> List("width"),
    4 -> List(),
    5 -> List(),
    6 -> List("width"),
    7 -> List(),
    8 -> List("format"),
    9 -> List("angle"),
    10 -> List("sigma"),
    11 -> List("sigma"),
    12 -> List())

  /* Data for the computation of the new memory limit using the model:
   *
   *  1. the structure of the input vectors
   *  2. the function argument processors
   *  3. the model itself
   *
   * Paths are "relative" to the root of the Docker container image of the controller. They relate to files in
   * "core/controller".
   */
  final val dataStructure = loadDataStructure("/data_structure.arff")
  final val functionArgumentsProcessors = loadFunctionArguments("/argprocs")
  final val model = loadModel("/model_linsvm_lowerterms_5")

  /** Load the data structure of the input vectors for the model. */
  def loadDataStructure(inpath: String): Instances = {
    val fin = new FileInputStream(inpath)
    val arffLoader = new ArffLoader
    arffLoader.setSource(fin)
    val ret = arffLoader.getDataSet
    fin.close()

    ret
  }

  /** Load the argument processors of the functions, so we can process the arguments of the incoming invocation.
   *
   * To learn the model, we processed the values of the arguments of each function calls all in the same way so they
   * could be used in training the model. As a result, we need to process the arguments of the function for which we are
   * computing a new memory limit in the exact same way (for instance normalization or nominal arguments).
   */
  def loadFunctionArguments(inpath: String): ArgumentsProcessorFactories = {
    /* Here we load the serialized ArgumentsProcessorFactories from file, converting them to proper Scala objects, and also
     * mapping each factory to the name of its argument using FUNCTION_ARGUMENTS. This is specific to using the offline model.
     */
    val ois = new ObjectInputStream(new FileInputStream(inpath))
    val ret = ois.readObject
      .asInstanceOf[java.util.Map[Integer, java.util.List[ArgumentProcessorFactory]]]
      .asScala
      .map { case (index, value) => index -> FUNCTION_ARGUMENTS(index).zip(value.asScala).toMap }
      .toMap
    ois.close()

    ret
  }

  /** Load the model from disk. */
  def loadModel(inpath: String): Classifier = {
    SerializationHelper.read(inpath).asInstanceOf[Classifier]
  }

  /** Build the input vector to feed the model to compute the new memory limit. */
  def buildInvocationInstance(action: WhiskActionMetaData, payload: Option[JsObject]): Instance = {
    // input size, deployment, memory cap, user, function, language,        (6)
    // numeric arguments x5, nominal arguments x10 binary attributes each x5, allocated memory (class)
    val inst = new DenseInstance(NB_ATTRIBUTES)
    inst.setDataset(dataStructure)

    val arguments = payload.getOrElse(JsObject.empty).fields.mapValues {
      case JsString(arg)  => arg
      case JsNumber(arg)  => arg.toString
      case JsBoolean(arg) => arg.toString
      case JsNull         => ""
      case value          => throw new Exception(s"""Getting list of arguments from payload failed on JsValue "$value"""")
    }

    def getDeploymentId = {
      action.annotations.get("deployment") match {
        case Some(jsValue) =>
          jsValue match {
            case JsNumber(id) => id.doubleValue()
            case value =>
              throw new Exception(s"""Getting deployment ID from annotations failed: got JsValue "$value"""")
          }
        case None =>
          throw new Exception("""Getting deployment ID from annotations failed: no annotation "deployment" found""")
      }
    }

    def getUserId = {
      try {
        action.namespace.root.asString.substring(4).toDouble
      } catch {
        case err: NumberFormatException => throw new Exception("Getting user ID from root namespace failed", err)
      }
    }

    def getFunctionId = {
      action.annotations.get("function") match {
        case Some(jsValue) =>
          jsValue match {
            case JsNumber(id) => id.doubleValue()
            case value =>
              throw new Exception(s"""Getting function ID from annotations failed: got JsValue "$value"""")
          }
        case None =>
          throw new Exception("""Getting function ID from annotations failed: no annotation "function" found""")
      }
    }

    def getLanguage = {
      action.exec.kind.split(':').head match {
        case "python" => "python"
        case "nodejs" => "javascript"
        case "blackbox" =>
          List("python", "nodejs").find(action.exec.asInstanceOf[BlackBoxExecMetaData].image.name.contains(_)) match {
            case Some(language) =>
              language match {
                case "python" => "python"
                case "nodejs" => "javascript"
              }
            case None =>
              throw new Exception(s"""Getting the language from the blackbox image "${action.exec
                .asInstanceOf[BlackBoxExecMetaData]
                .image
                .name}" failed""")
          }
        case kind => throw new Exception(s"""Getting the language from the kind failed: unknown kind "$kind"""")
      }
    }

    // commented lines are what should be used with the actual system (and not the pre-learned off line model)
    inst.setValue(0, arguments("input_bytesize"))
//      inst.setValue(1, action.fullyQualifiedName(false).hashCode)
    inst.setValue(1, getDeploymentId)
    inst.setValue(2, action.limits.memory.megabytes)
//      inst.setValue(3, action.namespace.root.hashCode)
    inst.setValue(3, getUserId)
//      inst.setValue(4, action.name.hashCode)
    inst.setValue(4, getFunctionId)
    inst.setValue(5, getLanguage)

    6 until NB_ATTRIBUTES foreach { i =>
      inst.setValue(i, 0)
    }

    // note that it silently ignores unexpected arguments (including input_bytesize)
    functionArgumentsProcessors(FUNCTION_NAMES_TO_IDS(action.name.asString)).foreach {
      case (_, null) => // ignored argument
      case (argname, argprocfact) =>
        val argproc = argprocfact.build(arguments(argname.toString))

        inst.setValue(6 + argproc.getProcessedAttributeId, argproc.getProcessedValue)
    }

    inst
  }

  override def slim(action: WhiskActionMetaData, payload: Option[JsObject], memory: Option[String])(logging: Logging): Int = {
    try {
      val memPred = model.classifyInstance(buildInvocationInstance(action, payload)).ceil.longValue().B

      logging.info(this, s"Predicted memory: ${memPred.toMB} MiB")

      // Here we check that the regressed value is legal. The model predicts values between 128MiB and 3GiB which is a much
      // wider range than the default configuration of OW, so change that when deploying OW.
      MemoryLimit(
        if (memPred < MemoryLimit.MIN_MEMORY) MemoryLimit.MIN_MEMORY
        else if (memPred > MemoryLimit.MAX_MEMORY) MemoryLimit.MAX_MEMORY
        else memPred).megabytes
    } catch {
      case err: Exception =>
        logging.error(this, s"Predicting the memory limit failed\n${err.printStackTrace()}")
        action.limits.memory.megabytes
    }
  }
}
