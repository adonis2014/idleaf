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
	 * ��ȡ�������ݿ�����ݿ�����.
	 * @return ���ݿ�����
	 */
	public java.sql.Connection getConnection()
	{
		try
		{
			if (sqlConOnline == null)
			{
				sqlConOnline = sourceOnline.getConnection();
				// ddb��֧��
				//sqlConOnline.setTransactionIsolation(2);//���ø��뼶�����ύ
			}
		}
		catch (Exception e)
		{
			LOGGER.fatal("��ȡ���ݿ������쳣,errorMsg=" + e.getMessage(), e);
			sqlConOnline = null;
		}

		return sqlConOnline;
	}

	/**
	 * �ͷ����ݿ�����
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
			LOGGER.fatal("�ͷ����ݿ������쳣,errorMsg=" + e.getMessage(), e);
		}
		finally
		{
			sqlRst = null;
			pStmt = null;
			sqlConOnline = null;
		}
	}
}
