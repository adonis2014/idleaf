package com.zhuzhong.idleaf.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

/**
 *
 * @author hzwumsh
 * 创建时间 2018-04-02 15:16
 *
 */
public class BaseDao
{
	private static final String QUERY_SQL = "select step ,max_id from SEQGEN where biz_tag=?";
	private static final String UPDATE_SQL = "update SEQGEN set max_id=? where biz_tag=? and max_id=?";
	private static final Logger LOGGER = Logger.getLogger(BaseDao.class);
	private DataSource dataSource;

	public BaseDao(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	public IdSegment updateAndGetNextIdSegment(String bizTag)
	{
		DaoHelper daoHelper = new DaoHelper(dataSource);
		try
		{
			Connection connection = daoHelper.getConnection();
			daoHelper.pStmt = connection.prepareStatement(QUERY_SQL);
			daoHelper.pStmt.setString(1, bizTag);
			daoHelper.sqlRst = daoHelper.pStmt.executeQuery();

			if (daoHelper.sqlRst.next())
			{
				long step = daoHelper.sqlRst.getLong("step");
				long maxId = daoHelper.sqlRst.getLong("max_id");
				IdSegment nextIdSegment = new IdSegment(bizTag, step + maxId, step);
				// need close stmt
				closeStatement(daoHelper.pStmt);
				daoHelper.pStmt = connection.prepareStatement(UPDATE_SQL);
				daoHelper.pStmt.setLong(1, nextIdSegment.getMaxId());
				daoHelper.pStmt.setString(2, nextIdSegment.getBizTag());
				daoHelper.pStmt.setLong(3, maxId);
				int expectUpdateCount = daoHelper.pStmt.executeUpdate();
				if (expectUpdateCount == 1)
				{
					return nextIdSegment;
				}
				LOGGER.fatal("更新序列号影响行数不为1,bizTag=" + bizTag + ",expectUpdateCount=" + expectUpdateCount);
			}
		}
		catch (Exception e)
		{
			String msg = "查询序列号异常,bizTag=" + bizTag + ",errorMsg=" + e.getMessage();
			LOGGER.fatal(msg, e);
			throw new RuntimeException(msg, e);
		}
		finally
		{
			daoHelper.releaseConnection();
		}
		throw new RuntimeException("bizTag=" + bizTag + ",记录不存在");
	}

	private void closeStatement(PreparedStatement preparedStatement)
	{
		try
		{
			preparedStatement.close();
		}
		catch (SQLException e)
		{
		}
	}
}
