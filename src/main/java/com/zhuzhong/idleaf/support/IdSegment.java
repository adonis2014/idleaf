package com.zhuzhong.idleaf.support;

import java.util.concurrent.atomic.AtomicLong;

/**
 *
 * @author hzwumsh
 * 创建时间 2018-04-02 15:11
 *
 */
public class IdSegment
{
	private String bizTag;
	private long minId;
	private long maxId;

	private long step;

	private long middleId;

	private AtomicLong currentId;

	public IdSegment(String bizTag, long maxId, long step)
	{
		this.bizTag = bizTag;
		this.maxId = maxId;
		this.step = step;
		this.middleId = this.maxId - (long) Math.round(step / 2);
		this.minId = this.maxId - this.step;
		this.currentId = new AtomicLong(this.minId);
	}

	public long getMiddleId()
	{
		return middleId;
	}

	public long getMinId()
	{
		return minId;
	}

	public long getMaxId()
	{
		return maxId;
	}

	public long getStep()
	{
		return step;
	}

	public String getBizTag()
	{
		return bizTag;
	}

	public AtomicLong getCurrentId()
	{
		return currentId;
	}
}
