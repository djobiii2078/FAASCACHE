package scala 

import scala._ 

object Main extends App {

   implicit val RamCloudTest: Wrapper = new Wrapper()

  def myIsDefined(s: String) : Boolean  = {
    return !Option(s).getOrElse("").isEmpty()
  }

  //override def main(args: Array[String]) {

    println ("======= Scala wrapper test for RAMCloud : ")
    
    if(!myIsDefined(args(1))) System.exit(0) 

    if(myIsDefined(args(1)) && myIsDefined(args(2)))  println ("== Using \n Locator : "+args(1)+"\n TableName : "+args(2))

    
    
    val locator = args(1) 
    val keyTest = "KeyTesting"
    val keyValue = "KeyValueTesting"

    val tableName = if(myIsDefined(args(2))) args(2) else "TableTest"

    val tableId = RamCloudTest.wrapperCreateTable(locator, args(1))
    
    RamCloudTest.wrapperWrite(args(1), tableName, keyTest, keyValue)
    
    println ("== Read from Rc : "+ RamCloudTest.wrapperRead(locator, tableName, keyTest))
    
    println ("== LocdistDockeration : "+ RamCloudTest.wrapperGetLocator(locator, tableName, keyTest))
    
    //Discarding table 

    RamCloudTest.wrapperDropTable(locator,tableName)

    println ("======= Testing Ended.")
 // }

}