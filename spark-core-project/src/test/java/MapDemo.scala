import java.text.{DecimalFormat, SimpleDateFormat}

/**
  * Author lzc
  * Date 2019/12/13 10:48
  */
object MapDemo {
    def main(args: Array[String]): Unit = {
        /*var map1 = Map("a" -> 1, "b" -> 1)
        val map2 = Map("b" -> 1, "c" -> 1)
        
        map1 = map1.foldLeft(map2) {
            case (map, (w, count)) =>
                map + (w -> (map.getOrElse(w, 0) + count))
        }
        println(map1)*/
        
        /*val list1 = List(30, 50, 70, 60, 10, 20)
        val list2: List[Int] = list1.slice(0, list1.length - 1)
        val list3: List[Int] = list1.slice(1, list1.length)
        println(list2.zip(list3).map(x => s"${x._1}->${x._2}"))*/
//        val formatter = new DecimalFormat(".00%")
        val formatter = new DecimalFormat("0000.00")
        println(formatter.format(1))
        println(formatter.format(200))
        println(formatter.format(2000))
        println(formatter.format(20000))
//        println(formatter.format(0.23656))
        // 0000 0001  0100 0099 0020 1000
    }
}
