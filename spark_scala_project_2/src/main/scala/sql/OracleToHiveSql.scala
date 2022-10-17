package sql

case class OracleToHiveSql() {
  val (table1_oracle,table2_oracle) = ("table1_oracle","table2_oracle")
  val (table1_hive,table2_hive) = ("table1_hive","table2_hive")

  val oracleTable1_sql : String = "select col1,col2,col3 from table1_oracle"
  val oracleTable2_sql : String = "select col1,col2,col3 from table2_oracle"

  val oracleTable1_transform : String = "select cast(col1 as int),upper(col2),col3 from "
  val oracleTable2_transform : String = "select lower(col1),col2,col3 from "
}