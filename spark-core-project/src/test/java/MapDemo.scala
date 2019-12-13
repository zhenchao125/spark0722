/**
  * Author lzc
  * Date 2019/12/13 10:48
  */
object MapDemo {
    def main(args: Array[String]): Unit = {
        var map1 = Map("a" -> 1, "b" -> 1)
        val map2 = Map("b" -> 1, "c" -> 1)
        
        map1 = map1.foldLeft(map2) {
            case (map, (w, count)) =>
                map + (w -> (map.getOrElse(w, 0) + count))
        }
        println(map1)
    }
}
