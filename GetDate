import java.time.{LocalDate, DayOfWeek, WeekFields, DateTimeFormatter}
import java.util.Locale

def generateWeeklyData(year: Int, hierarchy_type: String): Seq[(String, String, String, String, String, String, String, String, String, String, String)] = {
  val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
  val firstDayOfYear = LocalDate.of(year, 1, 1)
  val weekFields = WeekFields.of(Locale.getDefault())

  (1 to firstDayOfYear.range(weekFields.weekOfWeekBasedYear()).getMaximum.toInt).map { week =>
    val weekStartDate = firstDayOfYear.`with`(weekFields.weekOfWeekBasedYear(), week.toLong)
    val weekEndDate = weekStartDate.`with`(DayOfWeek.FRIDAY)
    val weekNumber = f"$week%02d"
    val weekDesc = s"$year $weekNumber"

    ("WEEKLY", "Weekly Periods", "0", s"W_$year", s"$year", weekEndDate.format(formatter), weekDesc, "0", "W", hierarchy_type)
  }
}

val lastYear = LocalDate.now().getYear - 1

val result = generateWeeklyData(lastYear, "kyvos_gwm_pc")
result.foreach(println)
