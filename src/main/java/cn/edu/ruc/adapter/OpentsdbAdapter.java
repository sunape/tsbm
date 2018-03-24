package cn.edu.ruc.adapter;

import cn.edu.ruc.base.TsDataSource;
import cn.edu.ruc.base.TsParamConfig;
import cn.edu.ruc.base.TsQuery;
import cn.edu.ruc.base.TsWrite;
import cn.edu.ruc.db.Status;

public class OpentsdbAdapter implements DBAdapter {

	@Override
	public void initDataSource(TsDataSource ds,TsParamConfig tspc) {
		// TODO Auto-generated method stub

	}

	@Override
	public Object preWrite(TsWrite tsWrite) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status execWrite(Object write) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object preQuery(TsQuery tsQuery) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Status execQuery(Object query) {
		// TODO Auto-generated method stub
		return null;
	}

}
