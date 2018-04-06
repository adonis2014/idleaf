package com.zhuzhong.idleaf.support;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class DaoHelper
{
	private static final Logger LOGGER = Logger.getLogger(DaoHelper.class);
	public java.sql.PreparedStatement pStmt = null;
	public java.sql.ResultSet sqlRst = null;
	private java.sql.Connection sqlConOnline = null;
	private DataSource sourceOnline = null;

	public DaoHelper(DataSource sourceOnline)
	{
		this.sourceOnline = sourceOnline;
	}

	/**
	 * 获取在线数据库的数据库连接.
	 * @return 数据库连接
	 */
	public java.sql.Connection getConnection()
	{
		try
		{
			if (sqlConOnline == null)
			{
				sqlConOnline = sourceOnline.getConnection();
				// ddb不支持
				//sqlConOnline.setTransactionIsolation(2);//设置隔离级别会读提交
			}
		}
		catch (Exception e)
		{
			LOGGER.fatal("获取数据库连接异常,errorMsg=" + e.getMessage(), e);
			sqlConOnline = null;
		}

		return sqlConOnline;
	}

	/**
	 * 释放数据库连接
	 *
	 */
	public void releaseConnection()
	{
		try
		{
			if (sqlRst != null)
				sqlRst.close();
			if (pStmt != null)
				pStmt.close();
			if (sqlConOnline != null)
			{
				sqlConOnline.close();
			}
		}
		catch (Exception e)
		{
			LOGGER.fatal("释放数据库连接异常,errorMsg=" + e.getMessage(), e);
		}
		finally
		{
			sqlRst = null;
			pStmt = null;
			sqlConOnline = null;
		}
	}
}
