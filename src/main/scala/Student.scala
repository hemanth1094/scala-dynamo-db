import com.gu.scanamo.Table

trait StudentApi {

  val student = Table[Student]("student")

}

case class Student(id: Int, name: String, address: Option[Address] = None, subjects: List[String] = Nil)

case class Address(village: String, state: String)
