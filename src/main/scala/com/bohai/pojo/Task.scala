package com.bohai.pojo

/**
  * Created by liukai on 2020/10/13.
  */
class Task {

  var task_id: Long = _
  var task_name: String = _
  var table_name: String = _
  var dep_tables: String = _

  def this(task_id: Long, task_name: String, table_name: String, dep_tables: String) {
    this()
    this.task_id = task_id
    this.table_name = table_name
    this.table_name = table_name
    this.dep_tables = dep_tables
  }


  override def toString = s"Task($task_id, $task_name, $table_name, $dep_tables)"
}
