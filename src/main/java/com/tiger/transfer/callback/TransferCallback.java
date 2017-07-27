package com.tiger.transfer.callback;

import java.io.IOException;
import java.sql.SQLException;

public interface TransferCallback {
	
	/**
	 * 功能：迁移数据
	 * @param task
	 * @throws SQLException
	 * @throws IOException
	 */
	public void transferData(Object task) throws SQLException, IOException;
	
}
