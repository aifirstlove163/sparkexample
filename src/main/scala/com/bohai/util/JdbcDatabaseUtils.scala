// Copyright 2012,2013,2015 the original author or authors. All rights reserved.
// site: http://www.ganshane.com
package com.bohai.util

import java.sql._
import javax.sql.DataSource

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import net.sf.log4jdbc.ConnectionSpy
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

/**
  * jdbc操作数据库的通用类
  * Created by liukai on 2020/10/9.
  */
object JdbcDatabaseUtils {
  private val logger = LoggerFactory getLogger getClass

  def buildDataSource: DataSource = {
    val hikariConfig = new HikariConfig()
    hikariConfig.setDriverClassName(ConfigurationManager.getProperty("database.driver"))
    hikariConfig.setJdbcUrl(ConfigurationManager.getProperty("database.url"))
    hikariConfig.setUsername(ConfigurationManager.getProperty("database.user"))
    hikariConfig.setPassword(ConfigurationManager.getProperty("database.password"))
    hikariConfig.setConnectionTestQuery("select 1 from dual")
    //设置自动提交事务
    hikariConfig.setAutoCommit(false)
    hikariConfig.addDataSourceProperty("cachePrepStmts", "true")
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", "250")
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
    hikariConfig.setMaximumPoolSize(5)

    val dataSource = new HikariDataSource(hikariConfig) {
      override def getConnection: Connection = {
        new ConnectionSpy(super.getConnection)
      }
    }
    dataSource
  }

  def use[T](autoCommit: Boolean = true)(action: Connection => T)(implicit ds: DataSource): T = {
    val conn = getConnection(ds)
    try {
      conn.setAutoCommit(autoCommit)
      val ret = action(conn)
      if (!autoCommit) conn.commit()
      ret
    } catch {
      case NonFatal(e) =>
        e.printStackTrace()
        if (!autoCommit) conn.rollback()
        throw new Exception
    } finally {
      closeJdbc(conn)
    }
  }

  def update(sql: String)(psSetter: PreparedStatement => Unit)(implicit ds: DataSource): Int = {
    use(autoCommit = true) { conn =>
      val st = conn.prepareStatement(sql)
      try {
        psSetter.apply(st)
        st.executeUpdate()
      } finally {
        closeJdbc(st)
      }
    }
  }

  def closeJdbc(resource: Any) {
    if (resource == null) return
    try {
      resource match {
        case c: Connection =>
          c.close()
        case s: Statement =>
          s.close()
        case r: ResultSet =>
          r.close()
        case _ => // do nothing
      }
    } catch {
      case NonFatal(e) => logger.error(e.getMessage, e)
    }
  }

  def queryAll(sql: String)(mapper: ResultSet =>Unit)(implicit ds: DataSource): Unit ={
    use(autoCommit = false){ conn =>
      val st = conn.prepareStatement(sql)
      try{
        val rs = st.executeQuery()
        try{
          while (rs.next) mapper(rs)
        }finally {
          closeJdbc(rs)
        }
      }finally {
        closeJdbc(st)
      }
    }
  }

  def queryFirst[T](sql: String)(psSetter: PreparedStatement => Unit)(mapper: ResultSet => T)(implicit ds: DataSource): Option[T] = {
    use(autoCommit = false) { conn =>
      val st = conn.prepareStatement(sql)
      try {
        psSetter.apply(st)
        val rs = st.executeQuery
        try {
          if (rs.next) Some(mapper(rs)) else None
        } finally {
          closeJdbc(rs)
        }
      } finally {
        closeJdbc(st)
      }
    }
  }

  /**
    * 查询自己控制是否遍历，通过手动执行rs.next遍历结果
    *
    * @param sql
    * @param psSetter
    * @param mapper
    * @param ds
    * @return
    */
  def queryWithPsSetter2(sql: String)(psSetter: PreparedStatement => Unit)(mapper: ResultSet => Unit)(implicit ds: DataSource) {
    use(autoCommit = false) { conn =>
      val st = conn.prepareStatement(sql)
      try {
        psSetter.apply(st)
        val rs = st.executeQuery
        try {
          mapper(rs)
        } finally {
          closeJdbc(rs)
        }
      } finally {
        closeJdbc(st)
      }
    }
  }

  /**
    * 查询遍历所有的结果集,不用执行rs.next
    *
    * @param sql
    * @param psSetter
    * @param mapper
    * @param ds
    * @return
    */
  def queryWithPsSetter(sql: String)(psSetter: PreparedStatement => Unit)(mapper: ResultSet => Unit)(implicit ds: DataSource) {
    use(autoCommit = false) { conn =>
      val st = conn.prepareStatement(sql)
      try {
        psSetter.apply(st)
        val rs = st.executeQuery
        try {
          while (rs.next) mapper(rs)
        } finally {
          closeJdbc(rs)
        }
      } finally {
        closeJdbc(st)
      }
    }
  }

  def getConnection(ds: DataSource) = ds.getConnection
}

