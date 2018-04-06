package com.zhuzhong.idleaf.support;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import javax.sql.DataSource;

import com.zhuzhong.idleaf.IdLeafService;
import org.apache.log4j.Logger;

/**
 *
 * @author hzwumsh
 * ����ʱ�� 2018-04-02 15:10
 *
 */
public class MysqlIdLeafServiceImpl implements IdLeafService
{
	private static final Logger LOGGER = Logger.getLogger(MysqlIdLeafServiceImpl.class);

	private volatile Future<Boolean> asynLoadSegmentTask = null;
	private volatile IdSegment[] segment = new IdSegment[2]; // �����������洢ÿ������֮������ֵ
	// ���ò����޸ģ����Բ���Ҫ����Ϊvolatile
	private AtomicBoolean switchFlag = new AtomicBoolean(true);
	private ReentrantLock lock = new ReentrantLock();
	// �����̳߳�
	private ExecutorService taskExecutor;
	private DataSource dataSource;
	private BaseDao baseDao;
	private String bizTag;
	private boolean asynLoadingSegment;

	public void setTaskExecutor(ExecutorService taskExecutor)
	{
		this.taskExecutor = taskExecutor;
	}

	public void init()
	{
		if (this.bizTag == null)
		{
			throw new RuntimeException("bizTag must be not null");
		}
		if (this.dataSource == null)
		{
			throw new RuntimeException("jdbcTemplate must be not null");
		}
		baseDao = new BaseDao(this.dataSource);
		if (taskExecutor == null)
		{
			taskExecutor = Executors.newSingleThreadExecutor();
		}
		segment[0] = doUpdateNextSegment(bizTag);
		setSwitchFlag(false);
		LOGGER.info(this.bizTag + " init run success...");
	}

	private Long asynGetId()
	{
		IdSegment currentIdSegment = segment[index()];
		long nextId = currentIdSegment.getCurrentId().incrementAndGet();
		// ʹ��50%��ʱ�򴥷���ȡ����
		if (currentIdSegment.getMiddleId() <= nextId
				&& (asynLoadSegmentTask == null || currentIdSegment.getMaxId() <= nextId))
		{
			try
			{
				lock.lock();
				if (currentIdSegment.getMiddleId() <= nextId
						&& currentIdSegment.getMinId() == segment[index()].getMinId() && asynLoadSegmentTask == null)
				{
					asynLoadSegmentTask = taskExecutor.submit(new Callable<Boolean>() {

						@Override
						public Boolean call() throws Exception
						{
							try
							{
								final int reCurrentIndex = reIndex();
								segment[reCurrentIndex] = doUpdateNextSegment(bizTag);
								return true;
							}
							catch (Throwable e)
							{
								LOGGER.fatal("�첽��ȡ���������쳣,bizTag=" + bizTag + ", errorMsg=" + e.getMessage(), e);
								return false;
							}
						}

					});
				}

				if (currentIdSegment.getMaxId() <= nextId && segment[index()].getMaxId() == currentIdSegment.getMaxId())
				{
					try
					{
						boolean loadingResult = asynLoadSegmentTask.get(500, TimeUnit.MILLISECONDS);
						if (!loadingResult)
						{
							throw new RuntimeException("�첽�����������зǳɹ�");
						}
						setSwitchFlag(!isSwitchFlag()); // �л�
					}
					catch (Throwable e)
					{
						LOGGER.error("���̳߳ػ�ȡ���������쳣,bizTag=" + bizTag + ", errorMsg=" + e.getMessage(), e);
						// ǿ��ͬ���л�
						forceUpdateSegment();
						setSwitchFlag(!isSwitchFlag()); // �л�
					}
					finally
					{
						asynLoadSegmentTask = null;
					}
				}

				if (nextId > currentIdSegment.getMaxId())
				{
					currentIdSegment = segment[index()];
					nextId = currentIdSegment.getCurrentId().incrementAndGet();
				}

				if (nextId > currentIdSegment.getMaxId())
				{
					// ǿ��ͬ���л�
					forceUpdateSegment();
					setSwitchFlag(!isSwitchFlag()); // �л�
					currentIdSegment = segment[index()];
					nextId = currentIdSegment.getCurrentId().incrementAndGet();
				}
			}
			finally
			{
				lock.unlock();
			}
		}

		if (nextId > currentIdSegment.getMaxId())
		{
			throw new RuntimeException("��ȡ��һ�����г�����ǰ���з�Χ");
		}
		return nextId;
	}

	private Long synGetId()
	{
		IdSegment currentIdSegment = segment[index()];
		long nextId = currentIdSegment.getCurrentId().incrementAndGet();
		if (currentIdSegment.getMiddleId() <= nextId)
		{
			try
			{
				lock.lock();

				if (currentIdSegment.getMiddleId() <= nextId
						&& segment[index()].getMinId() == currentIdSegment.getMinId()
						&& (segment[reIndex()] == null || segment[reIndex()].getMinId() < currentIdSegment.getMinId()))
				{
					// ʹ��50%���м���
					final int currentIndex = reIndex();
					segment[currentIndex] = doUpdateNextSegment(bizTag);
				}

				if (currentIdSegment.getMaxId() <= nextId && segment[index()].getMinId() == currentIdSegment.getMinId())
				{
					// ��һ�λ�Ϊ���سɹ�
					if (segment[reIndex()] == null || segment[reIndex()].getMinId() < currentIdSegment.getMinId())
					{
						forceUpdateSegment();
					}
					setSwitchFlag(!isSwitchFlag()); // �л�
				}

				if (nextId > currentIdSegment.getMaxId())
				{
					currentIdSegment = segment[index()];
					nextId = currentIdSegment.getCurrentId().incrementAndGet();
				}

				if (nextId > currentIdSegment.getMaxId())
				{
					// ǿ��ͬ���л�
					forceUpdateSegment();
					setSwitchFlag(!isSwitchFlag()); // �л�
					currentIdSegment = segment[index()];
					nextId = currentIdSegment.getCurrentId().incrementAndGet();
				}
			}
			finally
			{
				lock.unlock();
			}
		}

		if (nextId > currentIdSegment.getMaxId())
		{
			throw new RuntimeException("��ȡ��һ�����г�����ǰ���з�Χ");
		}

		return nextId;
	}

	// just for monitor
	private void forceUpdateSegment()
	{
		// ǿ��ͬ���л�
		final int reCurrentIndex = reIndex();
		segment[reCurrentIndex] = doUpdateNextSegment(bizTag);
	}

	private int reIndex()
	{
		if (isSwitchFlag())
		{
			return 0;
		}
		else
		{
			return 1;
		}
	}

	@Override
	public Long getId()
	{
		if (asynLoadingSegment)
		{
			return asynGetId();
		}
		else
		{
			return synGetId();
		}
	}

	private boolean isSwitchFlag()
	{
		return switchFlag.get();
	}

	private void setSwitchFlag(boolean switchFlag)
	{
		for (int index = 0; index < 3; index++)
		{
			if (this.switchFlag.compareAndSet(!switchFlag, switchFlag))
			{
				return;
			}
		}
		throw new RuntimeException("�޸��л���־ʧ��,����,bizTag=" + bizTag);
	}

	private int index()
	{
		if (isSwitchFlag())
		{
			return 1;
		}
		else
		{
			return 0;
		}
	}

	private IdSegment doUpdateNextSegment(String bizTag)
	{
		try
		{
			return updateId(bizTag);
		}
		catch (Exception e)
		{
			LOGGER.error("��һ�λ�ȡ���������쳣,bizTag=" + bizTag + ", errorMsg=" + e.getMessage(), e);
			try
			{
				return updateId(bizTag);
			}
			catch (Exception e1)
			{
				LOGGER.error("�ڶ��λ�ȡ���������쳣,bizTag=" + bizTag + ", errorMsg=" + e1.getMessage(), e1);
				throw e1;
			}
		}
	}

	private IdSegment updateId(String bizTag)
	{
		IdSegment nextIdSegment = baseDao.updateAndGetNextIdSegment(bizTag);
		if (nextIdSegment != null)
		{
			return nextIdSegment;
		}
		throw new RuntimeException("��ȡ��������ʧ��,bizTag=" + bizTag);
	}

	public void setDataSource(DataSource dataSource)
	{
		this.dataSource = dataSource;
	}

	public void setBizTag(String bizTag)
	{
		this.bizTag = bizTag;
	}

	public void setAsynLoadingSegment(boolean asynLoadingSegment)
	{
		this.asynLoadingSegment = asynLoadingSegment;
	}
}
