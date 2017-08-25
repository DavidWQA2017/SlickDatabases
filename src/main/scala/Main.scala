import slick.lifted.TableQuery
import slick.driver.MySQLDriver.api._

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

object Main extends App {
  // The config string refers to mysqlDB that we defined in application.conf
  val db = Database.forConfig("DataBase_1")

  // represents the actual table on which we will be building queries onval peopleTable = TableQuery[People]
  val peopleTable = TableQuery[People]

  // schema definition to generate DROP statement for people table
  val dropPeopleCmd = DBIO.seq(peopleTable.schema.drop)
  // schema definition to generate a CREATE TABLE command
  val initPeopleCmd = DBIO.seq(peopleTable.schema.create)
  listPeople
  //addPerson
  MostPopularFirstName




  def dropDB = {
    //do a drop followed by initialisePeople
    val dropFuture = Future {
      db.run(dropPeopleCmd)
    }
    //Attempt to drop the table, Await does not block here
    Await.result(dropFuture, Duration.Inf).andThen {
      case Success(_) => initialisePeople
      case Failure(error) =>
        println("Dropping the table failed due to: " + error.getMessage)
        initialisePeople
    }
  }

  def initialisePeople = {
    // /initialise people
    val setupFuture = Future {
      db.run(initPeopleCmd)
    }
    // once our DB has finished initializing we are ready to roll, Await does not block
    Await.result(setupFuture, Duration.Inf).andThen {
      case Success(_) => runQuery
      case Failure(error) =>
        println("Initialising the table failed due to: " + error.getMessage)
    }
  }

  def runQuery = {
    val insertPeople = Future {
      val query = peopleTable ++= Seq(
        (10, "Jack", "Wood", 36),
        (20, "Tim", "Brown", 24))
      // insert into `PEOPLE` (`PER_FNAME`,`PER_LNAME`,`PER_AGE`)  values (?,?,?)
      println(query.statements.head) // would print out the query one line up
      db.run(query)
    }

    Await.result(insertPeople, Duration.Inf).andThen {
      case Success(_) => listPeople
      case Failure(error) => println("Welp! Something went wrong! " + error.getMessage)
    }
  }

  def listPeople = {
    val queryFuture = Future {
      // simple query that selects everything from People and prints them out
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age) => println(s" $id $fName $lName $age")
      })
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => db.close() //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def FindPeople = {
    val queryFuture = Future {
      // simple query that finds a person
      db.run(peopleTable.result).map(_.foreach {
        case (id, fName, lName, age) if(fName == "Jack" && lName == "Wood") => println(s" $id $fName $lName $age")
      })
    }
    Await.result(queryFuture, Duration.Inf).andThen {
      case Success(_) => db.close() //cleanup DB connection
      case Failure(error) => println("Listing people failed due to: " + error.getMessage)
    }
  }

  def updatePerson =
  {
    val queryFuture = Future {
      // simple query that selects name and updates age
     val q = for (c <- peopleTable if c.fName  === "Jack") yield c.age
      val updateAction = q.update(36)
      db.run(updateAction)
    }


    Await.result(queryFuture, Duration.Inf).andThen
    {
      case Success(_) => db.close() //cleanup DB connection
      case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
    }

  }

  def deletePerson =
  {
    val queryFuture = Future {
      // simple query that deletes
      val q = peopleTable.filter(_.fName === "Jack")
      val deleteAction = q.delete
      db.run(deleteAction)
    }


    Await.result(queryFuture, Duration.Inf).andThen
    {
      case Success(_) => db.close() //cleanup DB connection
      case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
    }

  }
  def addPerson =
    {
      val queryFuture = Future {
        // simple query that adds or updates entry into system
        val addnewPerson = peopleTable.insertOrUpdate(30 ,"Tim" , "Wood" , 36)
        db.run(addnewPerson)
      }


      Await.result(queryFuture, Duration.Inf).andThen
      {
        case Success(_) => db.close() //cleanup DB connection
        case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
      }

    }

  def howManyRecordsQuery =
  {
    val queryFuture = Future {
      // simple query that counts amount of records
      var amount = peopleTable.length.result
      db.run(amount)
    }
    Await.result(queryFuture, Duration.Inf).andThen
    {
      case Success(amountOfRecords) => println(amountOfRecords); db.close()//println(amountOfRecords)//cleanup DB connection
      case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
    }
  }

  def averageAge =
  {
    val queryFuture = Future {
      // simple query that counts amount of records
      val ageAverage= peopleTable.map(_.age).avg.result
      db.run(ageAverage)
    }
    Await.result(queryFuture, Duration.Inf).andThen
    {
      case Success(ageAverage) => println(ageAverage); db.close()//println(amountOfRecords)//cleanup DB connection
      case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
    }
  }

  def  MostPopularFirstName =
  {
    val queryFuture = Future {
      // simple query that finds the most first names
      val lengths = peopleTable.groupBy(_.fName).map
      {
        case (s, results) => (s , results.length)
      }.sortBy(_._2.desc).result.head

      db.run(lengths)
    }
    Await.result(queryFuture, Duration.Inf).andThen
    {
      case Success(lengths) => println(lengths); db.close()//println(amountOfRecords)//cleanup DB connection
      case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
    }
  }

  def  MostPopularlastName =
  {
    val queryFuture = Future {
      // simple query that finds the most last names
      val lengths = peopleTable.groupBy(_.lName).map
      {
        case (s, results) => (s , results.length)
      }.sortBy(_._2.desc).result.head

      db.run(lengths)
    }
    Await.result(queryFuture, Duration.Inf).andThen
    {
      case Success(lengths) => println(lengths); db.close()//println(amountOfRecords)//cleanup DB connection
      case Failure(error) => println("Finding person has failed due to: " + error.getMessage)
    }
  }


}
