/**
  * Author lzc
  * Date 2019/12/16 9:18
  */
object Test {
    def main(args: Array[String]): Unit = {
        val arr1 = Array(30, 50, 70, 60, 10, 20)
        arr1(0) = 100
        arr1.update(1, 200)
        println(arr1.mkString(","))
    }
}
