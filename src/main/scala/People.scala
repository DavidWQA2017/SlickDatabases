import slick.driver.MySQLDriver.api._

class People(tag: Tag) extends Table[(Int, String, String, Int , String, String , Int)](tag, "PEOPLE")
{
  def id = column[Int]("PER_ID", O.PrimaryKey, O.AutoInc)
  def fName = column[String]("PER_FNAME")
  def lName = column[String]("PER_LNAME")
  def age = column[Int]("PER_AGE")
  def address = column[String]{"PER_ADDRESS"}
  def houseNumb = column[Int]{"PER_HOUSENUMB"}
  def city = column[String]{"PER_CITY"}
  def * = (id, fName, lName, age , address , city , houseNumb)
}
